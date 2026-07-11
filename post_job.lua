wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

function init(args)
    -- schedule 1 hour out so the sweeper doesn't grab jobs mid-test
    local scheduled = os.date("!%Y-%m-%dT%H:%M:%S.000Z", os.time() + 3600)

    local body = string.format(
        [[{"user":{"username":"sohan"},"scheduled_for":"%s","job":{"task":"send_email","priority":"high","payload":{"to":"test@example.com","subject":"hello"}}}]],
        scheduled
    )

    req = wrk.format("POST", "/createjob", nil, body)
end

function request()
    return req
end
