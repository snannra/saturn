wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

request_id = 0

function request()
    request_id = request_id + 1

    -- schedule 30 seconds in the future
    local scheduled_epoch = os.time() + 10

    -- ISO-ish UTC format
    local scheduled_for = os.date("!%Y-%m-%dT%H:%M:%S.000Z", scheduled_epoch)

    local body = string.format([[
    {
        "user": {
            "username": "sohan"
        },
        "scheduled_for": "%s",
        "job": {
            "task": "send_email",
            "priority": "high",
            "payload": {
            "to": "test%d@example.com",
            "subject": "hello"
            }
        }
    }
    ]], scheduled_for, request_id)

    return wrk.format(nil, "/createjob", nil, body)
end
