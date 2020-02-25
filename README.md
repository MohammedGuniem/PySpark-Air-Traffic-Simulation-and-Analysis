# dat500_project

### Follow the steps below to get started with this repository

# At first you need to create a new branch, this is done only once your try to do the first push
gi checkout -b <Your-prefered-branch-name>

# To push a single file, simply add it using the add command
git add file_name.txt

# To push all files, simply use the dot
git add .

# commit and write a usefull comment describing your commit
git commit -m "My comment"

# at last you need to run the push command
git push

# Type your username than your password to confirm


### Follow the steps below to pull in changes from master

# Set the upstream
git branch --set-upstream-to=origin/master <your-branch-name>
  
# pull changes from master branch
git pull
