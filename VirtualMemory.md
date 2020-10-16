## Virtual Memory
### How to read structure
`<list of something (0-N)>`
`[Field with fixed bytesize]`
`{Sub structure}`
### Structure
```
[4x8 byte Section-Header]
{Index}
{Data Section}
```
#### Section-Header
```
[8 byte block-Size]
[8 byte size-info]
[8 byte start-Address]
[8 byte end-Address]
```
#### Index
<3x8 byte Index-entry>
##### Index-entry
```
[8 byte Block size]
[8 byte End marker]
[8 byte end address]
```
### How is it working
A Virtual Memory is a random access memory split into `Sections`. This sections hold the user data and serve the purpose of easy seperation of data and better performance by decreasing shifting.

#### Sections
A section is a range in a memory holding data.
* A section has a `start / end address`, this addresses limit the current capacity of the section.
* A section also stores the information about its `current size` / how much space of the capacity is used
* Additionaly it saves a `block size`. The section is increased by this size when the capacity is fully used

By default this meta-data is stored in a 4x8 byte header, but the addresses where the different meta-fields are stored can be fully customized when documented
#### The index
The index saves the sections meta-data. The Start address is not stored as the start address is the end address of the previous section.
For the first Section the Start address is equal to the Index end address