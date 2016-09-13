<?php declare(strict_types=1);

namespace Prooph\EventStore\Adapter\Predis;

use DateTimeInterface;
use Iterator;
use Predis\Client;
use Predis\ClientContextInterface;
use Predis\ClientInterface;
use Predis\Transaction\MultiExec;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageDataAssertion;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Adapter\PayloadSerializer;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;

/**
 * @author Eric Braun <eb@oqq.be>
 */
final class PredisEventStoreAdapter implements Adapter, CanHandleTransaction
{
    const NAMESPACE_SEPARATOR = ':';

    /** @var Client */
    private $redis;

    /** @var MultiExec */
    private $transaction;

    /** @var MessageFactory */
    private $messageFactory;

    /** @var MessageConverter */
    private $messageConverter;

    /** @var PayloadSerializer */
    private $payloadSerializer;

    /** @var array */
    private static $standardColumns = ['event_id', 'event_name', 'created_at', 'payload', 'version'];

    /**
     * @param Client $predis
     * @param MessageFactory $messageFactory
     * @param MessageConverter $messageConverter
     * @param PayloadSerializer $payloadSerializer
     */
    public function __construct(
        ClientInterface $predis,
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        PayloadSerializer $payloadSerializer
    ) {
        $this->redis = $predis;
        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->payloadSerializer = $payloadSerializer;
    }

    /**
     * @inheritDoc
     */
    public function create(Stream $stream)
    {
        if (!$stream->streamEvents()->valid()) {
            throw new RuntimeException(
                sprintf(
                    "Cannot create empty stream %s. %s requires at least one event to extract metadata information",
                    $stream->streamName()->toString(),
                    __CLASS__
                )
            );
        }

        $this->appendTo($stream->streamName(), $stream->streamEvents());
    }

    /**
     * @inheritDoc
     */
    public function appendTo(StreamName $streamName, Iterator $streamEvents)
    {
        foreach ($streamEvents as $event) {
            $this->insertEvent($streamName, $event);
        }
    }

    /**
     * @inheritDoc
     */
    public function load(StreamName $streamName, $minVersion = null)
    {
        $events = $this->loadEvents($streamName, [], $minVersion);

        return new Stream($streamName, $events);
    }

    /**
     * @inheritDoc
     */
    public function loadEvents(StreamName $streamName, array $metadata = [], $minVersion = null)
    {
        $events = $this->getEventsWithMinVersion($this->getRedisKey($streamName), $minVersion);

        return $this->getMessages($events, $metadata);
    }

    /**
     * @inheritDoc
     */
    public function replay(StreamName $streamName, DateTimeInterface $since = null, array $metadata = [])
    {
        $events = $this->getEventsCreatedSince($this->getRedisKey($streamName), $since);

        return $this->getMessages($events, $metadata);
    }

    /**
     * @inheritdoc
     */
    public function beginTransaction()
    {
        if (null !== $this->transaction) {
            throw new \RuntimeException('Transaction already started');
        }

        $this->transaction = $this->redis->transaction();
    }

    /**
     * @inheritdoc
     */
    public function commit()
    {
        $this->assertActiveTransaction();

        $this->transaction->execute();
        $this->transaction = null;
    }

    /**
     * @inheritdoc
     */
    public function rollback()
    {
        $this->assertActiveTransaction();

        $this->transaction->discard();
        $this->transaction = null;
    }

    /**
     * @param StreamName $streamName
     * @param Message $event
     * @todo pipeline all requests in one commit
     * @todo provide redis key generator service
     */
    private function insertEvent(StreamName $streamName, Message $event)
    {
        $eventArr = $this->messageConverter->convertToArray($event);

        MessageDataAssertion::assert($eventArr);

        $eventData = [
            'event_id' => $eventArr['uuid'],
            'version' => $eventArr['version'],
            'event_name' => $eventArr['message_name'],
            'payload' => $this->payloadSerializer->serializePayload($eventArr['payload']),
            'created_at' => $eventArr['created_at']->format('Y-m-d\TH:i:s.u'),
        ];

        foreach ($eventArr['metadata'] as $key => $value) {
            $eventData[$key] = (string) $value;
        }

        $redisKey = $this->getRedisKey($streamName);

        if (isset($eventData['aggregate_id'])) {
            $aggregateKey = $this->getAggregateKey($redisKey, $eventData['aggregate_id']);

            $eventForVersion = $this->getClientContext()->zcount(
                $aggregateKey,
                $eventData['version'],
                $eventData['version']
            );

            if ($eventForVersion) {
                throw new ConcurrencyException('At least one event with same version exists already');
            }

            $this->getClientContext()->zadd(
                $aggregateKey,
                [$eventData['event_id'] => (int) $eventData['version']]
            );
        }

        $this->getClientContext()->zadd(
            $this->getVersionKey($redisKey),
            [$eventData['event_id'] => (int) $eventData['version']]
        );

        $this->getClientContext()->zadd(
            $this->getCreatedSinceKey($redisKey),
            [$eventData['event_id'] => (float) $eventArr['created_at']->format('U.u')]
        );

        $this->getClientContext()->hmset(
            $this->getEventDataKey($redisKey, $eventData['event_id']),
            $eventData
        );
    }

    /**
     * @return ClientInterface|ClientContextInterface
     */
    private function getClientContext()
    {
        return $this->transaction ?? $this->redis;
    }

    /**
     * @param StreamName $streamName
     *
     * @return string
     */
    private function getRedisKey(StreamName $streamName): string
    {
        return sprintf('{%s}', str_replace(['-', '\\'], self::NAMESPACE_SEPARATOR, $streamName->toString()));
    }

    /**
     * @param string $redisKey
     *
     * @return string
     */
    private function getVersionKey(string $redisKey): string
    {
        return $redisKey . self::NAMESPACE_SEPARATOR . 'version';
    }

    /**
     * @param string $redisKey
     *
     * @return string
     */
    private function getCreatedSinceKey(string $redisKey): string
    {
        return $redisKey . self::NAMESPACE_SEPARATOR . 'created_since';
    }

    /**
     * @param string $redisKey
     * @param string $eventId
     *
     * @return string
     */
    private function getEventDataKey(string $redisKey, string $eventId): string
    {
        return $redisKey . self::NAMESPACE_SEPARATOR . 'event_data' . self::NAMESPACE_SEPARATOR . $eventId;
    }

    /**
     * @param string $redisKey
     * @param string $aggregateId
     *
     * @return string
     */
    private function getAggregateKey(string $redisKey, string $aggregateId): string
    {
        return $redisKey . self::NAMESPACE_SEPARATOR . 'aggregate' . self::NAMESPACE_SEPARATOR . $aggregateId;
    }

    /**
     * @param string $redisKey
     * @param int|null $minVersion
     *
     * @return \Generator
     */
    private function getEventsWithMinVersion(string $redisKey, int $minVersion = null)
    {
        $eventIds = $this->redis->zrangebyscore(
            $this->getVersionKey($redisKey),
            $minVersion ?? 1,
            '+inf'
        );

        yield from $this->getEventsFromIds($redisKey, $eventIds);
    }

    /**
     * @param string $redisKey
     * @param DateTimeInterface|null $since
     *
     * @return \Generator
     */
    private function getEventsCreatedSince(string $redisKey, DateTimeInterface $since = null)
    {
        $eventIds = $this->redis->zrangebyscore(
            $this->getCreatedSinceKey($redisKey),
            null !== $since ? (float) $since->format('U.u') : 1,
            '+inf'
        );

        yield from $this->getEventsFromIds($redisKey, $eventIds);
    }

    /**
     * @param string $redisKey
     * @param array $eventIds
     *
     * @return \Generator
     */
    private function getEventsFromIds(string $redisKey, array $eventIds)
    {
        foreach ($eventIds as $eventId) {
            yield $this->redis->hgetall($this->getEventDataKey($redisKey, $eventId));
        }
    }

    /**
     * @param Iterator $events
     * @param array $metadata
     *
     * @return \Iterator
     * @todo: improve metadata filter
     */
    private function getMessages(\Iterator $events, array $metadata)
    {
        $messages = [];

        foreach ($events as $event) {
            foreach ($metadata as $key => $value) {
                if ($event[$key] !== $value) {
                    continue 2;
                }
            }

            $metadata = [];
            foreach ($event as $key => $value) {
                if (! in_array($key, self::$standardColumns)) {
                    $metadata[$key] = $value;
                }
            }

            $payload = $this->payloadSerializer->unserializePayload($event['payload']);
            $createdAt = \DateTimeImmutable::createFromFormat(
                'Y-m-d\TH:i:s.u',
                $event['created_at'],
                new \DateTimeZone('UTC')
            );

            $messages[] = $this->messageFactory->createMessageFromArray($event['event_name'], [
                'uuid' => $event['event_id'],
                'version' => (int) $event['version'],
                'created_at' => $createdAt,
                'payload' => $payload,
                'metadata' => $metadata
            ]);
        }

        return new \ArrayIterator($messages);
    }

    /**
     * @throws \RuntimeException
     */
    private function assertActiveTransaction()
    {
        if (null === $this->transaction) {
            throw new \RuntimeException('There is no active transaction');
        }
    }
}
