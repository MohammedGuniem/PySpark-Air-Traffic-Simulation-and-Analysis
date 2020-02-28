# dat500_project

## Follow the steps below to get started with this repository

### Since we are only 2 in this project, we will skip using branches and push directly to master branch, the following command is used to make sure we are on the master branch
gi checkout master

### To push a single file, simply add it using the add command
git add file_name.txt

### To push all files, simply use the dot
git add .

### commit and write a usefull comment describing your commit
git commit -m "My comment"

### at last you need to run the push command
git push

### Type your username than your password to confirm


## Follow the steps below to pull in changes from master

### Set the upstream, this is done only once you try to do the first pull
git branch --set-upstream-to=origin/master master
  
### pull changes from master branch
git pull

### Type your username than your password to confirm
