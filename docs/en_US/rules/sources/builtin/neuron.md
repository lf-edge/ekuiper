# Neuron Source

Neuron source is provided to consume events from the local neuron instance. Notice that, the source is bound to the local neuron only which must be able to communicate through nanomsg ipc protocol without network. In the eKuiper side, all neuron source and sink instances share the same connection, thus the events consumed are also the same. The events are formatted in json by default. The event content and format are fixed as below: 

```json
{
  "timestamp": 1646125996000,
  "node_name": "node1", 
  "group_name": "group1",
  "values": {
    "tag_name1": 11.22,
    "tag_name2": "string"
  },
  "errors": {
    "tag_name3": 122
  }
}
```

There is no configuration properties. An example of creating neuron source:

```text
CREATE STREAM table1 () WITH (FORMAT="json", TYPE="neuron");
```