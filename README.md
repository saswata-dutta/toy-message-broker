Producer
    has a Broker
    produce(mssg) => broker.submit(mssg)

Broker 
    has a Q, semaphores to control empty and filled
    has list of Consumers
    has a DAG

    submit(mssg) => for now block producer if Q is full

    push(mssg) => with topo order of DAG submit messages to them, wait retry times, fail and move on to nxt consumer 
    (if predecesor of consumer has failed lower consumers shouldnt reach ?, check immediate parents all passed)
    remove packet if reached end of chain

Consumer
    has an id
    has a list of parent id-s (may be empty)
    has a predicate canConsume(mssg) => bool
    consume(mssg) : return true if passed without exception


