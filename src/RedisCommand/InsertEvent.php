<?php declare(strict_types = 1);

namespace Prooph\EventStore\Adapter\Predis\RedisCommand;

use Predis\Command\ScriptCommand;

/**
 * @author Eric Braun <eb@burgeins.de>
 */
final class InsertEvent extends ScriptCommand
{
    /**
     * @inheritdoc
     */
    protected function getKeysCount()
    {
        return 4;
    }

    /**
     * @inheritDoc
     */
    public function getScript()
    {
        return <<<LUA
local versionKey = KEYS[1]
local createdSinceKey = KEYS[2]
local eventDataKey = KEYS[3]
local aggregateKey = KEYS[4]

local eventId = ARGV[1]
local version = ARGV[2]
local createdAt = ARGV[3]

redis.call('zadd', versionKey, version, eventId)
redis.call('zadd', createdSinceKey, createdAt, eventId)

if aggregateKey ~= '' then
    redis.call('zadd', aggregateKey, version, eventId)
end

redis.call('hmset', eventDataKey, unpack(ARGV, 4))

LUA;
    }
}
