import socket

client = socket.socket()
HOST = socket.gethostname()
PORT = 1234
ADDR = (HOST, PORT)
IP_ADDR = socket.gethostbyname(HOST)
SEPARATOR = '<SEPARATOR>'
FORMAT = 'utf-8'
CHUNK = 2048
date= '2001.02.02'
client.connect(ADDR)
request_options = ["I", "V", "B","P","U"]

while True:
    request = input(
            "I : Insertion of a new row\nV : View all Rows\nP:View Popular Movies\nU:Update the rating\nEnter option (B to break): ")

    if request in request_options:
        req_str = str(request)

        if request == "B":
            print('Client shutting down')
            break
        elif request=="U":
            movie_to_query=input('Enter movie where data will be updated with the rating you enter below')
            new_rating=input('Enter rating for the movie you entered')
            req_str=req_str+SEPARATOR+new_rating+SEPARATOR+movie_to_query
        elif request == "I":
            movietitle = input("Enter Movie Title : ")
            status = input("Enter status of movie : ")
            releasedate = input("Release date : ")
            revenue=(input("Enter revenue : "))
            voteaverage=input("Enter vote average : ")
            req_str = req_str + SEPARATOR + movietitle + SEPARATOR + status + SEPARATOR + releasedate + SEPARATOR + revenue + SEPARATOR +voteaverage
        client.send(req_str.encode(FORMAT))
        print('data sent')
        print(client.recv(CHUNK).decode(FORMAT))

    else:
        print("Provide a valid request")
    print("\n")

client.close()