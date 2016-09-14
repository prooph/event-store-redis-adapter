<?php declare(strict_types = 1);

namespace Prooph\EventStore\Adapter\Predis\RedisCommand;

use Predis\Command\ScriptCommand;

/**
 * @author Eric Braun <eb@burgeins.de>
 */
final class GetEventsByVersion extends ScriptCommand
{
    /**
     * @inheritdoc
     */
    protected function getKeysCount()
    {
        return 1;
    }

    /**
     * @param string $response
     *
     * @return array
     */
    public function parseResponse($response)
    {
        return json_decode($response, true);
    }

    /**
     * @inheritDoc
     */
    public function getScript()
    {
        return <<<LUA
local versionKey = KEYS[1]
local minVersion = ARGV[1]

local collate = function (key)
    local rawData = redis.call('hgetall', key)
    local result = {}
    
    for i = 1, #rawData, 2 do
        result[rawData[i]] = rawData[i + 1]
    end

    return result
end

local events = {}
local eventKeys = redis.call('zrangebyscore', versionKey, minVersion, '+inf')

for _, key in ipairs(eventKeys) do
    table.insert(events, collate(key))
end

return cjson.encode(events)
LUA;
    }
}
