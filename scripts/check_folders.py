import os
print("CWD:", os.getcwd())
print("Dir listing:", os.listdir("I:/ComAPIs"))
print("Utilities listing:", os.listdir("I:/ComAPIs/data/utilities") if os.path.exists("I:/ComAPIs/data/utilities") else "No utilities dir")
