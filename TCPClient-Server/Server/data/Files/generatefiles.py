
with open("100MB.txt", "wb") as f:
    f.seek(1024 * 1024 * 100 ) # One MB
    f.write('Hello World'.encode())
print("Created 100 MB file")


with open("250MB.txt", "wb") as f:
    f.seek(1024 * 1024 * 250 ) # One MB
    f.write('Hello World'.encode())

print("Created 250 MB file")
