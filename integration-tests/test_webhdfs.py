#
# Sample code for WebHDFS integration tests.
# Requires hadoop to be running on localhost, at the moment.
#
import smart_open

with smart_open.smart_open("webhdfs://localhost:50070/user/root/input/core-site.xml") as fin:
    print(fin.read())

with smart_open.smart_open("webhdfs://localhost:50070/user/root/input/test.txt") as fin:
    print(fin.read())

with smart_open.smart_open("webhdfs://localhost:50070/user/root/input/test.txt?user.name=root", 'wb') as fout:
    fout.write(b'hello world')
