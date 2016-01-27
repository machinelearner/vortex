Premise of vortex is to offer abstractions which help in taking away the data store heterogenity in many of the application built today

## vortex-core
The Core set of primitives which help vortex offer the common view of all the underlying store heterogenity.

#### Queries(VQuery)
Primitives which are used as the programming interface to express all kinds of query operations against any give data store

#### Target
Helps define the common level of interaction which any application will have w.r.t data-stores. Every target will understand how to convert a VQuery, primitive expressed by Vortex to Native notion of query to get desired result

#### Executors
Primitive which composes a pipeline of operations either pointed to one/many store or as a flow from source to sink. 
Look at TargetExecutor and FlowExecutor. Flow is a primitive which defines what and how of a particular flow.

#### Next Up
* Native Query execution as part of TargetExecutor
* Native Query execution as part of a Flow(tricky)
* Parallelize data Flow - PartitionedQuery(read and write), Read and Write orderering criterion, Actor?

#### Next release
* Parallelize flow - MessageDriven Query?
* Online Execution primitives
* Host as part of a stream - Notion of stream(event)
