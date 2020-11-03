# Backed Collections

![GitHub issues by-label](https://img.shields.io/github/issues/nbrugger-tgm/backed-collections/bug)![GitHub closed issues by-label](https://img.shields.io/github/issues-closed/nbrugger-tgm/backed-collections/bug)

![GitHub release (latest by date)](https://img.shields.io/github/v/release/nbrugger-tgm/backed-collections?label=latest%20stable)
![GitHub commits since latest release (by date)](https://img.shields.io/github/commits-since/nbrugger-tgm/backed-collections/latest)
![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/nbrugger-tgm/backed-collections?include_prereleases&label=latest)

![GitHub last commit](https://img.shields.io/github/last-commit/nbrugger-tgm/backed-collections)![GitHub Workflow Status](https://img.shields.io/github/workflow/status/nbrugger-tgm/backed-collections/Java%20JUnit%20Test%20with%20Gradle)<br>

This lib provides (currently) a backed list and map implementation.

### Development

![Lines of code](https://img.shields.io/tokei/lines/github/nbrugger-tgm/backed-collections)<br>

![Code Climate issues](https://img.shields.io/codeclimate/issues/nbrugger-tgm/backed-collections?label=Code%20Quality%20issues)
[![Maintainability](https://img.shields.io/codeclimate/maintainability/nbrugger-tgm/backed-collections.svg)](https://codeclimate.com/github/nbrugger-tgm/backed-collections)
[![Maintainability](https://img.shields.io/codeclimate/maintainability-percentage/nbrugger-tgm/backed-collections.svg)](https://codeclimate.com/github/nbrugger-tgm/backed-collections)

### Usage

### ![Java 1.9](https://img.shields.io/badge/java-1.9-blue)

### ![Maven metadata URL](https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Fniton.jfrog.io%2Fartifactory%2Fjava-libs%2Fcom%2Fniton%2Fbacked-collections%2Fmaven-metadata.xml)

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
* Managed Random access [more](Java Direct-Memory.md) (kind of the lower level of this lib -> powerfull)
* Open Interfaces(/abstract classes) for everyone
    * Create Custom `DataStore`s (Like `FileStore` or `ArrayStore`)
    * Custom Serializers

> `*` This means the collections data wont be damaged when your software chrashes. <b>!!!EXCEPT It fails during a write access.</b>

## Attention

Due to this type of collections are not intended to be created by Java there are a few things to keep in mind. All of them are very important as the collection classes dont behave like you expect them to. This said *all of my collection implementations are fully java spect compatible* and the behavior descibed bellow is not missbehaving but, unexpected as normaly this features do work.

* **HashCode** : The backed Map relies on HashCodes. The problem is that some Classes change their HashCode when they are serialized and deserialized which leads to the Keys having null values.
* **Reference Updating** : while you can do `list.get(0).setSomething(anything)` for normal collections, backed collections wont update the record as i cant capture it. you would need to `get()` an item, then `set` its property and than **replace** it with `set`

## Roadmap

- [ ] Full Javadoc
- [ ] Linked Map implementation (to overcome hashCode problems at the cost of performance)
- [ ] ! Performance optimisation
  - [x] Managed Storing
  - [ ] Collection Caching
- [ ] Set Implementation
- [ ] Stack, Queue Implementation
- [ ] Add Oberserver & Serilaizing Interface support
- [ ] Custom Performance options
