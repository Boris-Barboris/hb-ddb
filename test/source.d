//import ddb.pg.subscriber : PGSubscriber;
//PGSubscriber subscriber;

@safe:
void runTest() @safe
{
    import ddb.pg : connectPG , PGCommand;
    import std.process : environment;
    import std.stdio;
    import vibe.core.log;
    import vibe.core.core : runTask;
    setLogLevel(LogLevel.debug_);
    //setLogLevel(LogLevel.trace);

    auto pdb = connectPG([
        "host" : environment.get("DB_HOST", "localhost"),
        "database" : environment["DB_NAME"],
        "user" : environment["DB_USER"],
        "password" : environment["DB_PASSWORD"]
    ]);
    auto conn = pdb.lockConnection();

    // prepared statement
    {
        auto cmd = new PGCommand(conn, `SELECT * from "LoanRequests" Limit 1`);
        auto result = cmd.executeQuery();
        scope(exit) () @trusted { result.destroy; }();
        foreach (row; result)
            writeln(row);
    }

    // query
    with (conn.transaction) {
        auto result2 = conn.query(`SELECT * from "LoanRequests" Limit 1`);
        scope(exit) () @trusted { result2.destroy; }();
        foreach (row; result2)
            writeln(row);
    }

    conn.transaction({
        auto result2 = conn.query(`SELECT * from "LoanRequests" Limit 1`);
        scope(exit) () @trusted { result2.destroy; }();
        foreach (row; result2)
            writeln(row);
    });

	auto subscriber = pdb.createSubscriber();

    subscriber.subscribe("test1", "test2");
    auto task = subscriber.listen((string channel, string message) {
        writefln("channel: %s, msg: %s", channel, message);
    });

    conn.publish("test1", "Hello World!");
    conn.publish("test2", "Hello from Channel 2");

    runTask({
        subscriber.subscribe("test-fiber");
        subscriber.publish("test-fiber", "Hello from the Fiber!");
        subscriber.unsubscribe();
    });

    import vibe.core.core : sleep;
    import std.datetime : msecs;
	sleep(100.msecs);
}

int main()
{
    import vibe.core.core, vibe.core.log;
    int ret = 0;
    runTask({
        try runTest();
        catch (Throwable th) {
            logError("Test failed: %s", th.msg);
            logDiagnostic("Full error: %s", th);
            ret = 1;
        } finally exitEventLoop(true);
    });
    runEventLoop();
    return ret;
}
