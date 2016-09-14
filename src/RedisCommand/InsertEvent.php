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
local createdDateKey = KEYS[2]
local aggregateKey = KEYS[3]
local eventDataKey = KEYS[4]

local version = ARGV[1]
local createdAt = ARGV[2]

redis.call('zadd', versionKey, version, eventDataKey)
redis.call('zadd', createdDateKey, createdAt, eventDataKey)

if aggregateKey ~= '' then
    redis.call('zadd', aggregateKey, version, eventDataKey)
end

redis.call('hmset', eventDataKey, unpack(ARGV, 3))

return 1
LUA;
    }
}
