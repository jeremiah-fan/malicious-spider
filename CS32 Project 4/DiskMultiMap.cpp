#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "DiskMultiMap.h"
#include "BinaryFile.h"
#include "MultiMapTuple.h"
#include <functional>
#include <cstring>
#include <string>
using namespace std;

DiskMultiMap::DiskMultiMap() {
}

DiskMultiMap::~DiskMultiMap() {
	close();
}

bool DiskMultiMap::createNew(const string& filename, unsigned int numBuckets)
{
	close();
	b.createNew(filename);

	Header h;
	h.m_buckets = numBuckets;
	b.write(h, b.fileLength());

	for (int i = 0; i < h.m_buckets; i++) {
		Bucket bk;
		b.write(bk, b.fileLength());
	}

	return true;
}

bool DiskMultiMap::openExisting(const string& filename)
{
	close();
	return b.openExisting(filename);
}

void DiskMultiMap::close()
{
	if (b.isOpen())
		b.close();
}

BinaryFile::Offset DiskMultiMap::getNode() {
	Header h = getHeader();
	if (h.m_delHead == 0)
		return b.fileLength();

	BinaryFile::Offset temp = h.m_delHead;
	Node n;
	b.read(n, h.m_delHead);
	
	h.m_delHead = n.next;
	b.write(h, 0);

	return temp;
}

DiskMultiMap::Header DiskMultiMap::getHeader() {
	Header h;
	if (b.fileLength() != 0)
		b.read(h, 0);
	return h;
}

BinaryFile::Offset DiskMultiMap::getBucket(string key) {
	hash <string> hash;
	Header h = getHeader();
	return hash(key) % h.m_buckets*sizeof(Bucket) + sizeof(Header);
}

bool DiskMultiMap::insert(const string& key, const string& value, const string& context) {
	if (key.size() > 120 || value.size() > 120 || context.size() > 120)
		return false;

	BinaryFile::Offset loc = getNode();
	BinaryFile::Offset bucketLoc = getBucket(key);
	Bucket bk;
	b.read(bk, bucketLoc);

	Node n;
	strcpy(n.key, key.c_str());
	strcpy(n.value, value.c_str());
	strcpy(n.context, context.c_str());
	
	n.next = bk.m_head;
	b.write(n, loc);

	bk.m_head = loc;
	b.write(bk, bucketLoc);
	
	return true;
}

int DiskMultiMap::erase(const string&key, const string& value, const string& context) {
	int anyRemoved = 0;

	Bucket bk;
	BinaryFile::Offset bucketLoc = getBucket(key);
	b.read(bk, bucketLoc);

	BinaryFile::Offset curr = bk.m_head;
	BinaryFile::Offset prev = 0;

	Node cur;
	while (curr != 0)
	{
		b.read(cur, curr);
		if (strcmp(cur.key, key.c_str()) != 0
			|| strcmp(cur.value, value.c_str()) != 0
			|| strcmp(cur.context, context.c_str()) != 0)
		{
			prev = curr;
			curr = cur.next;
		}
		else
		{
			Node temp;
			b.read(temp, curr);
			Header h = getHeader();

			temp.next = h.m_delHead;
			b.write(temp, curr);

			h.m_delHead = curr;
			b.write(h, 0);

			curr = cur.next;
			if (prev == 0) {
				bk.m_head = curr;
				b.write(bk, bucketLoc);
			}
			else {
				Node pre;
				b.read(pre, prev);
				pre.next = curr;
				b.write(pre, prev);
			}

			anyRemoved++;
		}
	}

	return anyRemoved;
}

DiskMultiMap::Iterator DiskMultiMap::search(const string& key) {
	Bucket bk;
	b.read(bk, getBucket(key));

	BinaryFile::Offset curr = bk.m_head;
	BinaryFile::Offset prev = 0;
	Node cur;
	while (curr != 0) {
		b.read(cur, curr);
		if (strcmp(cur.key, key.c_str()) == 0) {
			if (prev == 0)
				return Iterator(&b, bk.m_head);
			Node temp;
			b.read(temp, prev);
			return Iterator(&b, temp.next);
		}

		prev = curr;
		curr = cur.next;
	}

	return Iterator(&b, 0);
}

bool DiskMultiMap::Iterator::isValid() const
{
	return m_node != 0;
}

DiskMultiMap::Iterator& DiskMultiMap::Iterator::operator++()
{
	if (!isValid())
		return *this;

	Node n;
	m_file->read(n, m_node);
	m_node = n.next;

	char s[121];
	strcpy(s, n.key);

	while (m_node != 0) {
		m_file->read(n, m_node);
		
		if (strcmp(n.key, s) == 0)
			break;

		m_node = n.next;
	}

	return *this;
}

MultiMapTuple DiskMultiMap::Iterator::operator*()
{
	MultiMapTuple m;
	if (!isValid())
		m.key = m.value = m.context = "";
	else {
		Node n;
		m_file->read(n, m_node);

		m.key = n.key;
		m.value = n.value;
		m.context = n.context;
	}
	return m;
}