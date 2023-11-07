# more details at: 
#   https://docs.python.org/3/library/asyncio-stream.html#examples

import asyncio, sys
import json

async def tcp_client(message):
    # print(message)
    reader, writer = \
        await asyncio.open_connection(
            '127.0.0.1', 5555)

    writer.write(json.dumps(message).encode())
    await writer.drain()

    empty_bytes = b''
    result = empty_bytes
    # cnt = 1
    while True:
        # print(cnt)
        # cnt += 1
        data = await reader.read(128000)
        # print(len(data))
        # print('start:', data[0:60], 'end:', data[-20:-1])
        # if data == empty_bytes:
        #     break
        result = result + data
        if len(data) < 128000:
            break
        # print(len(data))
    # if cnt:
    #     print('ju')
    # print(cnt)
    # print(result.decode())

    if message[0][1:] == 'get':
        # print(1)
        # print(f'{result.decode()!r}')
        with open(message[2], 'w') as f:
            f.write(result.decode())
    else:
        # pass
        print(result.decode())

    writer.close()



if len(sys.argv) == 4:
    func = sys.argv[1]
    if func[1:] == 'get':
        file_name = sys.argv[2]
        local_file_name = sys.argv[3]
        ms = [func, file_name, local_file_name]
    else:
        local_file_name = sys.argv[2]
        file_name = sys.argv[3]
        ms = [func, local_file_name, file_name]
else:
    func = sys.argv[1]
    file_name = sys.argv[2]
    ms = [func, file_name]

asyncio.run(tcp_client(ms))
