/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "proc_kv.h"

bool is_expired(const uint64_t future_timestamp, uint64_t& elapse_ms){
	uint64_t now = (uint64_t)time_ms();
	if (future_timestamp <= now) {
		return true;
	} else {
		elapse_ms = future_timestamp - now;
		return false;
	}
}

std::string add_timestamp_to_value(const Bytes &val, const uint64_t timestamp){
	uint64_t be = host_to_be(timestamp);
	std::string combine;
	combine.reserve(val.size() + sizeof(uint64_t));
	combine.append((char*)&be, sizeof(uint64_t));
	combine.append(val.data(), val.size());
	return combine;
}

// return 0: not found; -1: error; other ok val_without_stamp & elapse_ms
// if other and the key has no expire time, elapse_ms is UINT64_MAX
int get_val_ttl(SSDBImpl* ssdb, const Bytes &key, std::string* val_without_stamp, uint64_t &elaspe_ms) {
	std::string stamp_val;
	int ret = ssdb->get(key, &stamp_val);
	if (ret == -1 || ret == 0) {
		return ret;	// error -1 or not found 0 
	} 
	if (stamp_val.size() < sizeof(uint64_t)) {
		log_error("get_key_val_ttl timestamp_error %s", key.data());
		return -1;
	}
	uint64_t be = *((uint64_t*)stamp_val.data());
	if (be == 0) {
		elaspe_ms = UINT64_MAX;
		val_without_stamp->assign(stamp_val.data() + sizeof(uint64_t), stamp_val.size() - sizeof(uint64_t));		
	} else {
		uint64_t host = be_to_host(be);
		if (is_expired(host, elaspe_ms)) {
			return 0;	// expired as not found
		} else {
			val_without_stamp->assign(stamp_val.data() + sizeof(uint64_t), stamp_val.size() - sizeof(uint64_t));
		}
	}
	return 1;
}

void reply_get(const int ret, const std::string &val_without_stamp, Response *resp) {
	if (ret == 0 || ret == -1) {
		resp->reply_get(ret, NULL);
	} else {
		resp->reply_get(ret, &val_without_stamp);
	}
}

// return: -1 error, 0 not add (because key is empty), 1 add
int set_key_val(SSDBImpl* ssdb, const Bytes &key, const Bytes &val, const uint64_t &timestamp) {
	std::string stamp_val = add_timestamp_to_value(val, timestamp);
	return ssdb->set(key, Bytes(stamp_val));
}

uint64_t get_future_timestamp(const int ttl_second) {
	return time_ms() + (uint64_t)ttl_second * 1000;
}

int proc_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	// std::string val;
	// int ret = serv->ssdb->get(req[1], &val);

	std::string val_without_stamp;
	uint64_t elapse_ms;
	int ret = get_val_ttl(serv->ssdb, req[1], &val_without_stamp, elapse_ms);
	reply_get(ret, val_without_stamp, resp);
	return 0;
}

int proc_set(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	CHECK_KV_KEY_RANGE(1);

	//int ret = serv->ssdb->set(req[1], req[2]);

	int ret = set_key_val(serv->ssdb, req[1], req[2], 0);
	if(ret == -1){
		resp->push_back("error");
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
}

int proc_getset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	CHECK_KV_KEY_RANGE(1);

	// std::string val;
	// int ret = serv->ssdb->getset(req[1], &val, req[2]);
	// resp->reply_get(ret, &val);

	std::string old_val_without_stamp;
	uint64_t elapse_ms;
	int get_ret = get_val_ttl(serv->ssdb, req[1], &old_val_without_stamp, elapse_ms);
	int set_ret = set_key_val(serv->ssdb, req[1], req[2], 0);

	if (get_ret == 0 || get_ret == -1) {
		reply_get(get_ret, "", resp);
	} else {
		reply_get(set_ret, old_val_without_stamp, resp);
	}

	return 0;
}


int proc_setnx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	CHECK_KV_KEY_RANGE(1);

	// int ret = serv->ssdb->setnx(req[1], req[2]);

	std::string tmp_val;
	uint64_t tmp_elapse;
	int get_ret = get_val_ttl(serv->ssdb, req[1], &tmp_val, tmp_elapse);

	if (get_ret == -1) {
		resp->reply_bool(-1, "proc_setnx when get");
	} else if (get_ret != 0) {
		resp->reply_bool(0);
	} else {
		int set_ret = set_key_val(serv->ssdb, req[1], req[2], 0);
		if (set_ret == -1) {
			resp->reply_bool(-1, "proc_setnx when set");
		} else {
			resp->reply_bool(1);
		}		
	}

	// resp->reply_bool(ret);
	return 0;
}

int proc_setx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);
	CHECK_KV_KEY_RANGE(1);

	int ret = set_key_val(serv->ssdb, req[1], req[2], get_future_timestamp(req[3].Int()));

	if (ret == -1) {
		resp->push_back("error");
	} else {
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;

	/*
	Locking l(&serv->expiration->mutex);
	int ret;
	ret = serv->ssdb->set(req[1], req[2]);
	if(ret == -1){
		resp->push_back("error");
		return 0;
	}
	ret = serv->expiration->set_ttl(req[1], req[3].Int());
	if(ret == -1){
		resp->push_back("error");
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
	*/
}

int proc_ttl(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	//int64_t ttl = serv->expiration->get_ttl(req[1]);

	std::string val;
	uint64_t elapse_ms;
	int ret = get_val_ttl(serv->ssdb, req[1], &val, elapse_ms);

	int64_t ttl_second;
	if (ret == -1 || ret == 0) {
		ttl_second = -1;
	} else {
		ttl_second = (elapse_ms == UINT64_MAX) ? -1 : elapse_ms/1000;
	}

	resp->push_back("ok");
	resp->push_back(str(ttl_second));
	return 0;
}

int proc_expire(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	CHECK_KV_KEY_RANGE(1);

	std::string val;
	uint64_t elapse_ms;
	int get_ret = get_val_ttl(serv->ssdb, req[1], &val, elapse_ms);
	if (get_ret == -1) {
		resp->push_back("error");
	} else if (get_ret == 0) {
		resp->push_back("ok");
		resp->push_back("0");
	} else {
		int set_ret = set_key_val(serv->ssdb, req[1], Bytes(val), get_future_timestamp(req[2].Int()));
		if (set_ret == -1) {
			resp->push_back("error");
		} else {
			resp->push_back("ok");
			resp->push_back("1");
		}
	}
	return 0;

	/*
	Locking l(&serv->expiration->mutex);
	std::string val;
	int ret = serv->ssdb->get(req[1], &val);
	if(ret == 1){
		ret = serv->expiration->set_ttl(req[1], req[2].Int());
		if(ret != -1){
			resp->push_back("ok");
			resp->push_back("1");
		}else{
			resp->push_back("error");
		}
		return 0;
	}
	resp->push_back("ok");
	resp->push_back("0");
	return 0;
	*/
}

int proc_exists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	/*
	const Bytes key = req[1];
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	resp->reply_bool(ret);
	return 0;
	*/

	std::string val_without_stamp;
	uint64_t elapse_ms;
	int ret = get_val_ttl(serv->ssdb, req[1], &val_without_stamp, elapse_ms);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_exists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("multi_exist not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	resp->push_back("ok");
	for(Request::const_iterator it=req.begin()+1; it!=req.end(); it++){
		const Bytes key = *it;
		std::string val;
		int ret = serv->ssdb->get(key, &val);
		resp->push_back(key.String());
		if(ret == 1){
			resp->push_back("1");
		}else if(ret == 0){
			resp->push_back("0");
		}else{
			resp->push_back("0");
		}
	}
	return 0;
	*/
}

int proc_multi_set(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("multi_set not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() < 3 || req.size() % 2 != 1){
		resp->push_back("client_error");
	}else{
		int ret = serv->ssdb->multi_set(req, 1);
		resp->reply_int(0, ret);
	}
	return 0;
	*/
}

int proc_multi_del(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("multi_del not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	Locking l(&serv->expiration->mutex);
	int ret = serv->ssdb->multi_del(req, 1);
	if(ret == -1){
		resp->push_back("error");
	}else{
		for(Request::const_iterator it=req.begin()+1; it!=req.end(); it++){
			const Bytes key = *it;
			serv->expiration->del_ttl(key);
		}
		resp->reply_int(0, ret);
	}
	return 0;
	*/
}

int proc_multi_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("multi_get not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	resp->push_back("ok");
	for(int i=1; i<req.size(); i++){
		std::string val;
		int ret = serv->ssdb->get(req[i], &val);
		if(ret == 1){
			resp->push_back(req[i].String());
			resp->push_back(val);
		}
	}
	return 0;
	*/
}

int proc_del(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	int ret = serv->ssdb->del(req[1]);
	if(ret == -1){
		resp->push_back("error");
	} else {
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;

	/*
	Locking l(&serv->expiration->mutex);
	int ret = serv->ssdb->del(req[1]);
	if(ret == -1){
		resp->push_back("error");
	}else{
		serv->expiration->del_ttl(req[1]);
			
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
	*/
}

int proc_scan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("scan not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t limit = req[3].Uint64();
	KIterator *it = serv->ssdb->scan(req[1], req[2], limit);
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
		resp->push_back(it->val);
	}
	delete it;
	return 0;
	*/
}

int proc_rscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("rscan not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t limit = req[3].Uint64();
	KIterator *it = serv->ssdb->rscan(req[1], req[2], limit);
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
		resp->push_back(it->val);
	}
	delete it;
	return 0;
	*/
}

int proc_keys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("keys not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t limit = req[3].Uint64();
	KIterator *it = serv->ssdb->scan(req[1], req[2], limit);
	it->return_val(false);

	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
	}
	delete it;
	return 0;
	*/
}

int proc_rkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("rkeys not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t limit = req[3].Uint64();
	KIterator *it = serv->ssdb->rscan(req[1], req[2], limit);
	it->return_val(false);

	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key);
	}
	delete it;
	return 0;
	*/
}

// dir := +1|-1
static int _incr(SSDB *ssdb, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(2);
	int64_t by = 1;
	if(req.size() > 2){
		by = req[2].Int64();
	}
	int64_t new_val;
	int ret = ssdb->incr(req[1], dir * by, &new_val);
	if(ret == 0){
		resp->reply_status(-1, "value is not an integer or out of range");
	}else{
		resp->reply_int(ret, new_val);
	}
	return 0;
}

int proc_incr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("incr not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_KV_KEY_RANGE(1);
	return _incr(serv->ssdb, req, resp, 1);
	*/
}

int proc_decr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("decr not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_KV_KEY_RANGE(1);
	return _incr(serv->ssdb, req, resp, -1);
	*/
}

int proc_getbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("getbit not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	CHECK_KV_KEY_RANGE(1);

	int ret = serv->ssdb->getbit(req[1], req[2].Int());
	resp->reply_bool(ret);
	return 0;
	*/
}

int proc_setbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("setbit not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);
	CHECK_KV_KEY_RANGE(1);

	const Bytes &key = req[1];
	int offset = req[2].Int();
	if(req[3].size() == 0 || (req[3].data()[0] != '0' && req[3].data()[0] != '1')){
		resp->push_back("client_error");
		resp->push_back("bit is not an integer or out of range");
		return 0;
	}
	if(offset < 0 || offset > Link::MAX_PACKET_SIZE * 8){
		std::string msg = "offset is out of range [0, ";
		msg += str(Link::MAX_PACKET_SIZE * 8);
		msg += "]";
		resp->push_back("client_error");
		resp->push_back(msg);
		return 0;
	}
	int on = req[3].Int();
	int ret = serv->ssdb->setbit(key, offset, on);
	resp->reply_bool(ret);
	return 0;
	*/
}

int proc_countbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("countbit not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret == -1){
		resp->push_back("error");
	}else{
		std::string str;
		int size = -1;
		if(req.size() > 3){
			size = req[3].Int();
			str = substr(val, start, size);
		}else{
			str = substr(val, start, val.size());
		}
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
	*/
}

int proc_bitcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("bitcount not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	int end = -1;
	if(req.size() > 3){
		end = req[3].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret == -1){
		resp->push_back("error");
	}else{
		std::string str = str_slice(val, start, end);
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
	*/
}

int proc_substr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("substr not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	int size = 2000000000;
	if(req.size() > 3){
		size = req[3].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret == -1){
		resp->push_back("error");
	}else{
		std::string str = substr(val, start, size);
		resp->push_back("ok");
		resp->push_back(str);
	}
	return 0;
	*/
}

int proc_getrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("getrange not support!");
	return 0;

	/*
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	int size = -1;
	if(req.size() > 3){
		size = req[3].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret == -1){
		resp->push_back("error");
	}else{
		std::string str = str_slice(val, start, size);
		resp->push_back("ok");
		resp->push_back(str);
	}
	return 0;
	*/
}

int proc_strlen(NetworkServer *net, Link *link, const Request &req, Response *resp){

	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	CHECK_KV_KEY_RANGE(1);

	std::string val_without_stamp;
	uint64_t elapse_ms;
	int ret = get_val_ttl(serv->ssdb, req[1], &val_without_stamp, elapse_ms);

	if (ret == -1 || ret == 0) {
		resp->reply_int(ret, 0);
	} else {
		resp->reply_int(ret, val_without_stamp.size());
	}
	return 0;

	/*
	const Bytes &key = req[1];
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	resp->reply_int(ret, val.size());
	return 0;
	*/
}
