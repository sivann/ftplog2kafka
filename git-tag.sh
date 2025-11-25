#!/bin/bash



if [ "$#" -ne 1 ]
then
	echo ""
	echo "Usage: $0 <new version>"
	echo "e.g.: git-tag.sh 3.0"
	echo ""
	echo "Previous version:" 
	git tag  -l '[0-9].*'|tail -1
	exit 1
fi

newtag="$1"

if [[ $newtag =~ [0-9]+[.][0-9]+$ ]] ; then
	 echo "New version: $newtag" 
else
	echo "Invalid version: $newtag, aborting"
	exit 1
fi

if [ `git tag|grep $newtag` ] ; then
	echo "$newtag already exists, aborting"
	exit 2
fi

GITTOP=`git rev-parse --show-toplevel`
#Git version file (relative to git directory):
GVF=${GITTOP}/git_version.txt

DEF_VER=UNKNOWN

LF='
'

lastdate=`git log -1 --format=%ci`


if [ `git status --short|grep -v '^??'|wc -l` -ne 0 ] ; then
	git status --short
	echo "It seems there are not commited changes, aborting"
	exit 4
fi

read -p "Press [Enter] key to update version file: "
echo "${newtag}:${lastdate}" > ${GVF}
git add $GVF
git commit -a -m "updating $GVF to ${newtag}"

git push 

git tag -a $newtag -m "Version ${newtag}"

echo ""
git push --tags

