# create data directory if it does not exist
mkdir -p data/wikidump
# download latest wikipedia dump
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2 \
    -O data/wikidump/enwiki-latest-pages-articles-multistream.xml.bz2
# extract dump
bzip2 -d data/wikidump/enwiki-latest-pages-articles-multistream.xml.bz2
