import time
from smart_open.bytebuffer import ByteBuffer

buffer = ByteBuffer()

start = time.time()
for _ in range(1000):
    assert buffer.fill([b"X" * 1000]) == 1000
print(time.time() - start)
