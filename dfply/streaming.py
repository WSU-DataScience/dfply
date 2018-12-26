'''
GENERAL THOUGHTS

TYPES: We should probably add types and match pandas, which has 

>    The main types stored in pandas objects are float, int, bool, datetime64[ns], timedelta[ns], and object. In addition these dtypes have item sizes, e.g. int64 and int32.
>    By default integer types are int64 and float types are float64, REGARDLESS of platform (32-bit or 64-bit). The following will all result in int64 dtypes.
>    Numpy, however will choose platform-dependent types when creating arrays. The following WILL result in int32 on 32-bit platform.

We could use numpy types like Pandas(?)  This works because dfply requires
pandas and numpy

See: https://stackoverflow.com/questions/29245848/what-are-all-the-dtypes-that-pandas-recognizes

OPERATIONS: Think about making the 

ORDER: Think about using an OrderDict with the fieldnames 
'''



'''Modifier and Type 	Method and Description
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Object                              apply(int i)
                                    Returns the value at position i.
                                    Todd: This is __call__
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Row                                 copy()
                                    Make a copy of the current Row object.
                                    Todd: Need to think about how to copy
boolean                             equals(Object o) 
                                    Todd: This is __eq__
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
int                                 fieldIndex(String name)
                                    Returns the index of a given field name.
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Object                              get(int i)
                                    Returns the value at position i.
<T> T                               getAnyValAs(int i)
                                    Returns the value at position i.
<T> T                               getAs(int i)
                                    Returns the value at position i.
<T> T                               getAs(String fieldName)
                                    Returns the value of a given fieldName.
boolean                             getBoolean(int i)
                                    Returns the value at position i as a primitive boolean.
byte                                getByte(int i)
                                    Returns the value at position i as a primitive byte.
java.sql.Date                       getDate(int i)
                                    Returns the value at position i of date type as java.sql.Date.
java.math.BigDecimal                getDecimal(int i)
                                    Returns the value at position i of decimal type as java.math.BigDecimal.
double                              getDouble(int i)
                                    Returns the value at position i as a primitive double.
float                               getFloat(int i)
                                    Returns the value at position i as a primitive float.
int                                 getInt(int i)
                                    Returns the value at position i as a primitive int.
<K,V> java.util.Map<K,V>            getJavaMap(int i)
                                    Returns the value at position i of array type as a java.util.Map.
<T> java.util.List<T>               getList(int i)
                                    Returns the value at position i of array type as java.util.List.
long                                getLong(int i)
                                    Returns the value at position i as a primitive long.
<K,V> scala.collection.Map<K,V> 	getMap(int i)
                                    Returns the value at position i of map type as a Scala Map.
<T> scala.collection.Seq<T>         getSeq(int i)
                                    Returns the value at position i of array type as a Scala Seq.
short                               getShort(int i)
                                    Returns the value at position i as a primitive short.
String                              getString(int i)
                                    Returns the value at position i as a String object.
Row                                 getStruct(int i)
                                    Returns the value at position i of struct type as a Row object.
java.sql.Timestamp                  getTimestamp(int i)
                                    Returns the value at position i of date type as java.sql.Timestamp.
<T> scala.collection.immutable.Map<String,T> 	getValuesMap(scala.collection.Seq<String> fieldNames)
                                                Returns a Map consisting of names and values for the requested fieldNames For primitive types if value is null it returns 'zero value' specific for primitive ie.
int                                 hashCode() 
boolean                             isNullAt(int i)
                                    Checks whether the value at position i is null.
int                                 length()
                                    Number of elements in the Row.
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
String                              mkString()
                                    Displays all elements of this sequence in a string (without a separator).
String                              mkString(String sep)
                                    Displays all elements of this sequence in a string using a separator string.
String                              mkString(String start, String sep, String end)
                                    Displays all elements of this traversable or iterator in a string using start, end, and separator strings.
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
StructType                          schema()
                                    Schema for the row.
int                                 size()
                                    Number of elements in the Row.
scala.collection.Seq<Object>        toSeq()
                                    Return a Scala Seq representing the row.
String 	toString() '''

class Row(dict):
    ''' A class for representing a row of a table as a dictionary.

    Similar to the Row class in spark SQL'''
    def __init__(*args, **kwargs):
        super(Row, self).__init__(*args, **kwargs)

    def  anyNull(self):
        return any(v is None for v in self.values())


