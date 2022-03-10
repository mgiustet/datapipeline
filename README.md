# datapipeline
Non-blocking data pipeline in Python, to process data at your own pace.

This library provides you with three classes: Pipeline, Socket and Block. In short:
- a Block is an object which (abstractly) represents a data-processing thread in your program. It can get data in input and post results on one of its outputs. Inputs and outputs are handled through Sockets. A Block can also have no source Sockets (e.g. the video feed from a camera) or no output Sockets (e.g. a webpage that displays the processed video feed);
- a Socket is an object which handles data reading and writing. It belongs to a Block, which is the only one that can write on it. Blocks can connect to other Sockets (which will be called source Sockets) to get data in input, then process it and post results on one or more output Sockets;
- a Pipeline is the datum of some connected Blocks. The structure of a Pipeline can be loaded from an XML file or created dinamically.

# How it works
Here's a simple example: we create a Pipeline with three blocks linked in a chain. We specify the Pipeline structure in an XML file:

```xml
<?xml version="1.0" encoding="UTF-8"?>

<pipeline>
  <block id="source">
    <output id="a"></output>
  </block>

  <block id="worker">
    <source id="a"></source>

    <output id="b"></output>
  </block>

  <block id="consumer">
    <source id="b"></source>
  </block>
</pipeline>
```
The first Block, with ID "source", has a single output Socket with ID "a". The second, with ID "worker", takes data in input from the Socket with ID "a" and writes results on Socket "b". Finally, Block "consumer" reads data from Socket "b". Now we add the actual code: we load the structure from the XML file and we use three threads to actually create and process data:

```python
import random
import threading
import time
from datapipeline import *

delay = 0.25

text = ""
with open("test-config.xml", "r") as f
text = f.read()

p = Pipeline.parse_structure(text)

source_block = p.get_block_by_id("source")
worker_block = p.get_block_by_id("worker")
consumer_block = p.get_block_by_id("consumer")

def generate():
    for x in range(100):
        # simulate a variable load
        time.sleep((random.random() + 0.5) * delay)
        source_block.write(x, "a")

def mult_by_two():
    while True:
        data = worker_block.read("a")
        # simulate a variable load
        time.sleep((random.random() + 0.5) * delay)
        worker_block.write(data * 2, "b")


def print_data():
    while True:
        data = consumer_block.read("b")
        print(data)



if __name__ == "__main__":
    t1 = threading.Thread(target=generate)
    t2 = threading.Thread(target=mult_by_two)
    t3 = threading.Thread(target=print_data)
    t1.start()
    t2.start()
    t3.start()
```

If you'll run this example, you'll notice that some numbers are skipped. This is because, by design, each Block does not wait for the next one to finish.
