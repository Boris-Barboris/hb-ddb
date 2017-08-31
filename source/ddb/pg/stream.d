///
module ddb.pg.stream;

import std.bitmanip : nativeToBigEndian;
import std.datetime;

import ddb.pg.types;
import ddb.pg.exceptions;

@safe:

class PGStream
{
    version (Have_vibe_core)
    {
        import vibe.core.net : TCPConnection;
        import vibe.internal.array : BatchBuffer;

        private TCPConnection m_socket;
        private ubyte[] m_write_buf;    // buffer for batched writes

        @property TCPConnection socket()
        {
            return m_socket;
        }

        this(TCPConnection socket)
        {
            m_socket = socket;
            m_write_buf.reserve(4096);
        }

        @property TCPConnection conn() { return m_socket; }
        @property void conn(TCPConnection conn) { m_socket = conn; }
        @property void socket(TCPConnection conn) { m_socket = conn; }
    }
    else
    {
        import std.socket : Socket;

        private Socket m_socket;

        @property Socket socket()
        {
            return m_socket;
        }

        this(Socket socket)
        {
            m_socket = socket;
        }
    }

    /// checks whether the socket is still connected
    bool isAlive()
    {
        version(Have_vibe_core)
            return socket.connected;
        else
            return socket.isAlive;
    }

    package(ddb) void read(ubyte[] buffer)
    {
        version(Have_vibe_core)
        {
            try
            {
                m_socket.read(buffer);
            }
            catch (Exception e)
            {
                throw new PGTcpErrorException(e.msg);
            }
        }
        else
        {
            if (buffer.length > 0)
            {
                m_socket.receive(buffer);
            }
        }
    }

    void flush()
    {
        version(Have_vibe_core)
        {
            try
            {
                m_socket.write(m_write_buf);
            }
            catch (Exception e)
            {
                throw new PGTcpErrorException(e.msg);
            }
            m_write_buf.length = 0;
        }
        // under OS sockets we have already written our buffer
    }

    void write(scope ubyte[] x)
    {
        version(Have_vibe_core)
        {
            m_write_buf ~= x;
            //m_socket.write(x);
        }
        else
        {
            if (x.length > 0)
            {
                m_socket.send(x);
            }
        }
    }

    void write(bool x)
    {
        write(nativeToBigEndian(x)); // ubyte[]
    }

    void write(ubyte x)
    {
        write(nativeToBigEndian(x)); // ubyte[]
    }

    void write(short x)
    {
        write(nativeToBigEndian(x)); // ubyte[]
    }

    void write(int x)
    {
        write(nativeToBigEndian(x)); // ubyte[]
    }

    void write(long x)
    {
        write(nativeToBigEndian(x));
    }

    void write(float x)
    {
        write(nativeToBigEndian(x)); // ubyte[]
    }

    void write(double x)
    {
        write(nativeToBigEndian(x));
    }

    void writeString(string x)
    {
        ubyte[] ub = () @trusted { return cast(ubyte[])(x); }();
        write(ub);
    }

    void writeCString(string x)
    {
        writeString(x);
        write('\0');
    }

    void writeCString(char[] x)
    {
        write(cast(ubyte[])x);
        write('\0');
    }

    // https://www.postgresql.org/docs/9.5/static/datatype-datetime.html
    void write(const ref Date x)
    {
        write(cast(long)(x.dayOfGregorianCal - PGEpochDay));
    }

    void write(Date x)
    {
        write(cast(long)(x.dayOfGregorianCal - PGEpochDay));
    }

    void write(const ref TimeOfDay x)
    {
        write(cast(long)((x - PGEpochTime).total!"usecs"));
    }

    void write(const ref DateTime x) // timestamp
    {
        write(cast(long)((x - PGEpochDateTime).total!"usecs"));
    }

    void write(DateTime x) // timestamp
    {
        write(cast(long)((x - PGEpochDateTime).total!"usecs"));
    }

    void write(const ref SysTime x) // timestamptz
    {
        write(cast(long)((x - SysTime(PGEpochDateTime, UTC())).total!"usecs"));
    }

    // BUG: Does not support months
    void write(const ref core.time.Duration x) // interval
    {
        int months = cast(int)(x.split!"weeks".weeks/28);
        int days = cast(int)x.split!"days".days;
        long usecs = x.total!"usecs" - convert!("days", "usecs")(days);

        write(usecs);
        write(days);
        write(months);
    }

    void writeTimeTz(const ref SysTime x) // timetz
    {
        TimeOfDay t = cast(TimeOfDay)x;
        write(t);
        write(cast(int)0);
    }
}
