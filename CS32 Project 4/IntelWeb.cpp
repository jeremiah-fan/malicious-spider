#include "IntelWeb.h"
#include "BinaryFile.h"
#include "InteractionTuple.h"
#include "MultiMapTuple.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <set>
#include <queue>
#include <vector>
using namespace std;

bool operator<(const InteractionTuple& a, const InteractionTuple& b) {
	if (a.context < b.context)
		return true;
	else if (a.context > b.context)
		return false;
	else if (a.from < b.from)
		return true;
	else if (a.from > b.from)
		return false;
	else if (a.to < b.to)
		return true;
	return false;
}

bool IntelWeb::createNew(const string& filePrefix, unsigned int maxDataItems)
{
	close();
	return m_init.createNew(filePrefix + "-initiator-hash-table.data", maxDataItems * 4 / 3) && m_target.createNew(filePrefix + "-target-hash-table.data", maxDataItems * 4 / 3);
}

bool IntelWeb::openExisting(const std::string& filePrefix)
{
	close();
	if (!m_init.openExisting(filePrefix + "-initiator-hash-table.data") || !m_target.openExisting(filePrefix + "-target-hash-table.data")) {
		close();
		return false;
	}

	return true;
}

void IntelWeb::close()
{
	m_init.close();
	m_target.close();
}

bool IntelWeb::ingest(const std::string& telemetryFile) {
	ifstream inf(telemetryFile);
	string line;
	while (getline(inf, line))
	{
		istringstream iss(line);
		string key, value, context;
		iss >> context >> key >> value;

		m_init.insert(key, value, context);
		m_target.insert(value, key, context);
	}

	return true;
}

unsigned int IntelWeb::crawl(const vector<string>& indicators, unsigned int minPrevalenceToBeGood, vector<string>& badEntitiesFound, vector<InteractionTuple>& badInteractions) {
	set<string> badEnt;
	set<InteractionTuple> badInt;

	queue<string> toCheck;
	for (int i = 0; i < indicators.size(); i++)
		toCheck.push(indicators[i]);
	int guaranteedBad = indicators.size();

	while (!toCheck.empty()) {
		string s = toCheck.front();
		toCheck.pop();

		if (badEnt.find(s) != badEnt.end())
			continue;

		vector <InteractionTuple> valPerEntity;

		int numCount = 0;
		DiskMultiMap::Iterator it = m_init.search(s);
		while (it.isValid() && numCount < minPrevalenceToBeGood) {
			MultiMapTuple m = *it;
			valPerEntity.push_back(InteractionTuple(m.key, m.value, m.context));
			++it;
			++numCount;
		}

		it = m_target.search(s);
		while (it.isValid() && numCount < minPrevalenceToBeGood) {
			MultiMapTuple m = *it;
			valPerEntity.push_back(InteractionTuple(m.value, m.key, m.context));
			++it;
			++numCount;
		}

		if (numCount > 0 && (numCount < minPrevalenceToBeGood || guaranteedBad > 0)) {
			badEnt.insert(s);
			for (int i = 0; i < valPerEntity.size(); i++) {
				if (s == valPerEntity[i].from)
					toCheck.push(valPerEntity[i].to);
				else
					toCheck.push(valPerEntity[i].from);
				badInt.insert(valPerEntity[i]);
			}
		}
		guaranteedBad--;
	}
	
	badEntitiesFound.empty();
	for (set<string>::iterator it = badEnt.begin(); it != badEnt.end(); it++) {
		badEntitiesFound.push_back(*it);
	}

	badInteractions.empty();
	for (set<InteractionTuple>::iterator it = badInt.begin(); it != badInt.end(); it++) {
		badInteractions.push_back(*it);
	}

	return badEntitiesFound.size();
}

bool IntelWeb::purge(const string& entity) {
	int count = 0;
	DiskMultiMap::Iterator it = m_init.search(entity);
	while (it.isValid()) {
		MultiMapTuple m = *it;
		++it;
		count += m_init.erase(m.key, m.value, m.context);
	}

	it = m_target.search(entity);
	while (it.isValid()) {
		MultiMapTuple m = *it;
		++it;
		count += m_target.erase(m.key, m.value, m.context);
	}
	return count > 0;
}