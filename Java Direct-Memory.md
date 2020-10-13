# Java Direct-Memory

There are 3 levels of memory access:

* Native
* Semi-Managed
* Managed

## Native

**Class : ** DataStore

Native access is basically extended random access. This means you directly manipulate the raw byte array. Only because its native it does not means a performance increase. Indeed semi managed includes some performance improvements, so unless you have a good concept to minimize shifting and jumping you are most likely better of using semi-managed.

This layer provides: direct reading, direct writing, shifting, shift writing

## Semi Managed

**Class : ** Section

Semi Managed access manages enlargement and shifting automatically. But you still have to somehow manage them if you use multiple Sections in a dataStore. A data store needs an address to write and reads it config from/to.

A Section needs pointer to the start of the config it needs to store.

### Metadata - 16Bytes

* Blocksize: **4 Bytes**
* end-marker: **4 Bytes**
* start-address: **4 Bytes**
* end-address: **4 Bytes** 

This layer provides : automatic shifting and size management.

## Managed

This makes it as convenient as possible to use binary.

Is a section map. You can create many sections without a need of managing them you can simply access them by indexes.

### Layout

* Index config : *16 bytes*
* Index : section
* List of managed sections