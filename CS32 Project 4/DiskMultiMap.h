#ifndef DISKMULTIMAP_H_
#define DISKMULTIMAP_H_

#include <string>
#include "MultiMapTuple.h"
#include "BinaryFile.h"

class DiskMultiMap
{
public:

	class Iterator
	{
	public:
		Iterator() {
			m_file = nullptr;
			m_node = 0;
		}

		Iterator(BinaryFile* b, BinaryFile::Offset n) {
			m_file = b;
			m_node = n;
		}

		bool isValid() const;
		Iterator& operator++();
		MultiMapTuple operator*();

	private:
		BinaryFile::Offset m_node;
		BinaryFile* m_file;
	};

	DiskMultiMap();
	~DiskMultiMap();
	bool createNew(const std::string& filename, unsigned int numBuckets);
	bool openExisting(const std::string& filename);
	void close();
	bool insert(const std::string& key, const std::string& value, const std::string& context);
	Iterator search(const std::string& key);
	int erase(const std::string& key, const std::string& value, const std::string& context);

private:
	struct Header
	{
		int m_buckets;
		BinaryFile::Offset m_delHead = 0;
	};

	struct Bucket
	{
		BinaryFile::Offset m_head = 0;
	};

	struct Node
	{
		char key[121], value[121], context[121];
		BinaryFile::Offset next;
	};

	DiskMultiMap::Header getHeader();
	BinaryFile::Offset getBucket(std::string key);
	BinaryFile::Offset getNode();
	BinaryFile b;
};

#endif // DISKMULTIMAP_H_