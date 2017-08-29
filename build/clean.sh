rm -rf src/packages
find . -name bin -print0 | xargs -0 rm -rf
find . -name obj -print0 | xargs -0 rm -rf

