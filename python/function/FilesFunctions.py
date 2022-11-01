import os

def getFolderContent(sourcePath, folderList):
  for i in os.listdir(sourcePath):
    if os.path.isfile(os.path.join(sourcePath,i)):
      folderList.append(["file", os.path.join(sourcePath, i)])
  for i in os.listdir(sourcePath):
    if os.path.isdir(os.path.join(sourcePath,i)):
      folderList.append(["dir", os.path.join(sourcePath, i)])
      getFolderContent(os.path.join(sourcePath,i), folderList)