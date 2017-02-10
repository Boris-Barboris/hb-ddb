module ddb.messages;

import std.ascii : LetterCase;
import std.bitmanip : bigEndianToNative;
import std.conv : ConvException, to, text;
import std.datetime;
import std.traits;
import std.variant : Variant;
import std.uuid : UUID;

import ddb.db : DBRow, isNullable, isVariantN, nullableTarget;
import ddb.exceptions;
import ddb.pgconnection : PGConnection;
import ddb.types;
import ddb.utils;

struct Message
{
    PGConnection conn;
    char type;
    ubyte[] data;

    private size_t position = 0;

    T read(T, Params...)(Params p)
    {
        T value;
        read(value, p);
        return value;
    }

    void read()(out char x)
    {
        x = data[position++];
    }


    void read(Int)(out Int x) if((isIntegral!Int || isFloatingPoint!Int) && Int.sizeof > 1)
    {
        ubyte[Int.sizeof] buf;
        buf[] = data[position..position+Int.sizeof];
        x = bigEndianToNative!Int(buf);
        position += Int.sizeof;
    }

    string readCString()
    {
        string x;
        readCString(x);
        return x;
    }

    void readCString(out string x)
    {
        ubyte* p = data.ptr + position;

        while (*p > 0)
            p++;
		x = cast(string)data[position .. cast(size_t)(p - data.ptr)];
        position = cast(size_t)(p - data.ptr + 1);
    }

    string readString(int len)
    {
        string x;
        readString(x, len);
        return x;
    }

    void readString(out string x, int len)
	{
		x = cast(string)(data[position .. position + len]);
		position += len;
	}

    void read()(out bool x)
    {
        x = cast(bool)data[position++];
    }

    void read()(out ubyte[] x, int len)
    {
        enforce(position + len <= data.length);
        x = data[position .. position + len];
        position += len;
    }

    void read()(out UUID u) // uuid
    {
        ubyte[16] uuidData = data[position .. position + 16];
        position += 16;
        u = UUID(uuidData);
    }

    void read()(out Date x) // date
    {
        int days = read!int; // number of days since 1 Jan 2000
        x = PGEpochDate + dur!"days"(days);
    }

    void read()(out TimeOfDay x) // time
    {
        long usecs = read!long;
        x = PGEpochTime + dur!"usecs"(usecs);
    }

    void read()(out DateTime x) // timestamp
    {
        long usecs = read!long;
        x = PGEpochDateTime + dur!"usecs"(usecs);
    }

    void read()(out SysTime x) // timestamptz
    {
        long usecs = read!long;
        x = SysTime(PGEpochDateTime + dur!"usecs"(usecs), UTC());
        x.timezone = LocalTime();
    }

    // BUG: Does not support months
    void read()(out core.time.Duration x) // interval
    {
        long usecs = read!long;
        int days = read!int;
        int months = read!int;

        x = dur!"days"(days) + dur!"usecs"(usecs);
    }

    SysTime readTimeTz() // timetz
    {
        TimeOfDay time = read!TimeOfDay;
        int zone = read!int / 60; // originally in seconds, convert it to minutes
        Duration duration = dur!"minutes"(zone);
        auto stz = new immutable SimpleTimeZone(duration);
        return SysTime(DateTime(Date(0, 1, 1), time), stz);
    }

    T readComposite(T)()
    {
        alias DBRow!T Record;

        static if (Record.hasStaticLength)
        {
            alias Record.fieldTypes fieldTypes;

            static string genFieldAssigns() // CTFE
            {
                string s = "";

                foreach (i; 0 .. fieldTypes.length)
                {
                    s ~= "read(fieldOid);\n";
                    s ~= "read(fieldLen);\n";
                    s ~= "if (fieldLen == -1)\n";
                    s ~= text("record.setNull!(", i, ");\n");
                    s ~= "else\n";
                    s ~= text("record.set!(fieldTypes[", i, "], ", i, ")(",
                              "readBaseType!(fieldTypes[", i, "])(fieldOid, fieldLen)",
                              ");\n");
                    // text() doesn't work with -inline option, CTFE bug
                }

                return s;
            }
        }

        Record record;

        int fieldCount, fieldLen;
        uint fieldOid;

        read(fieldCount);

        static if (Record.hasStaticLength)
            mixin(genFieldAssigns);
        else
        {
            record.setLength(fieldCount);

            foreach (i; 0 .. fieldCount)
            {
                read(fieldOid);
                read(fieldLen);

                if (fieldLen == -1)
                    record.setNull(i);
                else
                    record[i] = readBaseType!(Record.ElemType)(fieldOid, fieldLen);
            }
        }

        return record.base;
    }
	mixin template elmnt(U : U[])
	{
		alias U ElemType;
	}
    private AT readDimension(AT)(int[] lengths, uint elementOid, int dim)
    {

        mixin elmnt!AT;

        int length = lengths[dim];

        AT array;
        static if (isDynamicArray!AT)
            array.length = length;

        int fieldLen;

        foreach(i; 0 .. length)
        {
            static if (isArray!ElemType && !isSomeString!ElemType)
                array[i] = readDimension!ElemType(lengths, elementOid, dim + 1);
            else
            {
                static if (isNullable!ElemType)
                    alias nullableTarget!ElemType E;
                else
                    alias ElemType E;

                read(fieldLen);
                if (fieldLen == -1)
                {
                    static if (isNullable!ElemType || isSomeString!ElemType)
                        array[i] = null;
                    else
                        throw new Exception("Can't set NULL value to non nullable type");
                }
                else
                    array[i] = readBaseType!E(elementOid, fieldLen);
            }
        }

        return array;
    }

    T readArray(T)()
        if (isArray!T)
    {
        alias multiArrayElemType!T U;

        // todo: more validation, better lowerBounds support
        int dims, hasNulls;
        uint elementOid;
        int[] lengths, lowerBounds;

        read(dims);
        read(hasNulls); // 0 or 1
        read(elementOid);

        if (dims == 0)
            return T.init;

        enforce(arrayDimensions!T == dims, "Dimensions of arrays do not match");
        static if (!isNullable!U && !isSomeString!U)
            enforce(!hasNulls, "PostgreSQL returned NULLs but array elements are not Nullable");

        lengths.length = lowerBounds.length = dims;

        int elementCount = 1;

        foreach(i; 0 .. dims)
        {
            int len;

            read(len);
            read(lowerBounds[i]);
            lengths[i] = len;

            elementCount *= len;
        }

        T array = readDimension!T(lengths, elementOid, 0);

        return array;
    }

    T readEnum(T)(int len)
    {
        string genCases() // CTFE
        {
            string s;

            foreach (name; __traits(allMembers, T))
            {
                s ~= text(`case "`, name, `": return T.`, name, `;`);
            }

            return s;
        }

        string enumMember = readString(len);

        switch (enumMember)
        {
            mixin(genCases);
            default: throw new ConvException("Can't set enum value '" ~ enumMember ~ "' to enum type " ~ T.stringof);
        }
    }

    T readBaseType(T)(uint oid, int len = 0)
    {
        auto convError(T)()
        {
            string* type = oid in baseTypes;
            return new ConvException("Can't convert PostgreSQL's type " ~ (type ? *type : to!string(oid)) ~ " to " ~ T.stringof);
        }

        switch (oid)
        {
            case 16: // bool
                static if (isConvertible!(T, bool))
                    return _to!T(read!bool);
                else
                    throw convError!T();
            case 26, 24, 2202, 2203, 2204, 2205, 2206, 3734, 3769: // oid and reg*** aliases
                static if (isConvertible!(T, uint))
                    return _to!T(read!uint);
                else
                    throw convError!T();
            case 21: // int2
                static if (isConvertible!(T, short))
                    return _to!T(read!short);
                else
                    throw convError!T();
            case 23: // int4
                static if (isConvertible!(T, int))
                    return _to!T(read!int);
                else
                    throw convError!T();
            case 20: // int8
                static if (isConvertible!(T, long))
                    return _to!T(read!long);
                else
                    throw convError!T();
            case 700: // float4
                static if (isConvertible!(T, float))
                    return _to!T(read!float);
                else
                    throw convError!T();
            case 701: // float8
                static if (isConvertible!(T, double))
                    return _to!T(read!double);
                else
                    throw convError!T();
            case 1042, 1043, 25, 19, 705: // bpchar, varchar, text, name, unknown
                static if (isConvertible!(T, string))
                    return _to!T(readString(len));
                else
                    throw convError!T();
            case 17: // bytea
                static if (isConvertible!(T, ubyte[]))
                    return _to!T(read!(ubyte[])(len));
                else
                    throw convError!T();
            case 2950: // UUID
                static if(isConvertible!(T, UUID))
                    return _to!T(read!UUID());
                else
                    throw convError!T();
            case 18: // "char"
                static if (isConvertible!(T, char))
                    return _to!T(read!char);
                else
                    throw convError!T();
            case 1082: // date
                static if (isConvertible!(T, Date))
                    return _to!T(read!Date);
                else
                    throw convError!T();
            case 1083: // time
                static if (isConvertible!(T, TimeOfDay))
                    return _to!T(read!TimeOfDay);
                else
                    throw convError!T();
            case 1114: // timestamp
                static if (isConvertible!(T, DateTime))
                    return _to!T(read!DateTime);
                else
                    throw convError!T();
            case 1184: // timestamptz
                static if (isConvertible!(T, SysTime))
                    return _to!T(read!SysTime);
                else
                    throw convError!T();
            case 1186: // interval
                static if (isConvertible!(T, core.time.Duration))
                    return _to!T(read!(core.time.Duration));
                else
                    throw convError!T();
            case 1266: // timetz
                static if (isConvertible!(T, SysTime))
                    return _to!T(readTimeTz);
                else
                    throw convError!T();
            case 2249: // record and other composite types
                static if (isVariantN!T && T.allowed!(Variant[]))
                    return T(readComposite!(Variant[]));
                else
                    return readComposite!T;
            case 2287: // _record and other arrays
                static if (isArray!T && !isSomeString!T)
                    return readArray!T;
                else static if (isVariantN!T && T.allowed!(Variant[]))
                    return T(readArray!(Variant[]));
                else
                    throw convError!T();
            case 114: //JSON
                static if (isConvertible!(T, string))
                    return _to!T(readString(len));
                else
                    throw convError!T();
            default:
                if (oid in conn.arrayTypes)
                    goto case 2287;
                else if (oid in conn.compositeTypes)
                    goto case 2249;
                else if (oid in conn.enumTypes)
                {
                    static if (is(T == enum))
                        return readEnum!T(len);
                    else static if (isConvertible!(T, string))
                        return _to!T(readString(len));
                    else
                        throw convError!T();
                }
        }

        throw convError!T();
    }
}

/**
Class encapsulating errors and notices.

This class provides access to fields of ErrorResponse and NoticeResponse
sent by the server. More information about these fields can be found
$(LINK2 http://www.postgresql.org/docs/9.0/static/protocol-error-fields.html,here).
*/
class ResponseMessage
{
    package(ddb) string[char] fields;

    private string getOptional(char type)
    {
        string* p = type in fields;
        return p ? *p : "";
    }

    /// Message fields
    @property string severity()
    {
        return fields['S'];
    }

    /// ditto
    @property string code()
    {
        return fields['C'];
    }

    /// ditto
    @property string message()
    {
        return fields['M'];
    }

    /// ditto
    @property string detail()
    {
        return getOptional('D');
    }

    /// ditto
    @property string hint()
    {
        return getOptional('H');
    }

    /// ditto
    @property string position()
    {
        return getOptional('P');
    }

    /// ditto
    @property string internalPosition()
    {
        return getOptional('p');
    }

    /// ditto
    @property string internalQuery()
    {
        return getOptional('q');
    }

    /// ditto
    @property string where()
    {
        return getOptional('W');
    }

    /// ditto
    @property string file()
    {
        return getOptional('F');
    }

    /// ditto
    @property string line()
    {
        return getOptional('L');
    }

    /// ditto
    @property string routine()
    {
        return getOptional('R');
    }

    /**
    Returns summary of this message using the most common fields (severity,
    code, message, detail, hint)
    */
    override string toString()
    {
        string s = severity ~ ' ' ~ code ~ ": " ~ message;

        string* detail = 'D' in fields;
        if (detail)
            s ~= "\nDETAIL: " ~ *detail;

        string* hint = 'H' in fields;
        if (hint)
            s ~= "\nHINT: " ~ *hint;

        return s;
    }
}