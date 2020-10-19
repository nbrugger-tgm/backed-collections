# Backed Collections
This lib provides (currently) a backed list and map implementation.
### Example
Creating a map backed by a file
```java
File file = new File("file_containing_the_map.db");
FileStore store = new FileStore(file);

Map<String,Rectangle> m = new BackedMap(store,false);
m.put("Test",null);
```
<br>Opening a file containing a map
```java
File file = new File("file_containing_the_map.db");
FileStore store = new FileStore(file);

Map<String,Rectangle> m = new BackedMap(store,true);
Rectangle result = m.get("Test");
```
<br>
Split file for multiple Collections

````java
//Using a file as backing option
File file = new File("file_containing_the_map.db");
FileStore store = new FileStore(file);

//Splitting the file into multiple parts
VirtualMemory memory = new VirtualMemory(store);
Section mapSection = memory.createSection(50,4);//This values only change performance impact
Section listSection = memory.createSection(50,4);

//Using the sections to store the maps (still into the file)
Map<String,Rectangle> m = new BackedMap(mapSection,false);
List<String> l = new BackedList(store,false);

//Using the map and list as if they were default java lists
...
````

### Features
* Live Backing
* Failsavety`*`
* Backing to
    * File
    * Stack/Heap (RAM)
* Using a File for multiple collections
* Managed Random access [more](VirtualMemory.md) (kind of the lower level of this lib -> powerfull)
* Open Interfaces(/abstract classes) for everyone
    * Create Custom `DataStore`s (Like `FileStore` or `ArrayStore`)
    * Custom Serializers

> `*` This means the collections data wont be damaged when your software chrashes. <b>!!!EXCEPT It fails during a write access.</b>