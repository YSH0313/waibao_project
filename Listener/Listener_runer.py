import socket
from Listener.Listener_config import Listen

if __name__ == '__main__':
    listen = Listen(300)
    listen.start_run()
    # print(socket.gethostbyname(socket.gethostname()))