# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: raft.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nraft.proto\"\xaa\x01\n\x14\x41ppendEntriesRequest\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x10\n\x08leaderId\x18\x02 \x01(\x05\x12\x14\n\x0cprevLogIndex\x18\x03 \x01(\x05\x12\x13\n\x0bprevLogTerm\x18\x04 \x01(\x05\x12\x1a\n\x07\x65ntries\x18\x05 \x03(\x0b\x32\t.LogEntry\x12\x14\n\x0cleaderCommit\x18\x06 \x01(\x05\x12\x15\n\rleaseDuration\x18\x07 \x01(\x03\"f\n\x12\x41ppendEntriesReply\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x0f\n\x07success\x18\x02 \x01(\x08\x12\x17\n\x0f\x63onflictingTerm\x18\x03 \x01(\x05\x12\x18\n\x10\x63onflictingIndex\x18\x04 \x01(\x05\"\x82\x01\n\x12RequestVoteRequest\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x13\n\x0b\x63\x61ndidateId\x18\x02 \x01(\x05\x12\x14\n\x0clastLogIndex\x18\x03 \x01(\x05\x12\x13\n\x0blastLogTerm\x18\x04 \x01(\x05\x12\x1e\n\x16oldLeaderLeaseDuration\x18\x05 \x01(\x03\"5\n\x10RequestVoteReply\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x13\n\x0bvoteGranted\x18\x02 \x01(\x08\"+\n\x08LogEntry\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x11\n\toperation\x18\x02 \x01(\t\"%\n\x12ServeClientRequest\x12\x0f\n\x07request\x18\x01 \x01(\t\"C\n\x10ServeClientReply\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\t\x12\x10\n\x08leaderId\x18\x02 \x01(\x05\x12\x0f\n\x07success\x18\x03 \x01(\x08\x32\xbb\x01\n\x08RaftNode\x12=\n\rAppendEntries\x12\x15.AppendEntriesRequest\x1a\x13.AppendEntriesReply\"\x00\x12\x37\n\x0bRequestVote\x12\x13.RequestVoteRequest\x1a\x11.RequestVoteReply\"\x00\x12\x37\n\x0bServeClient\x12\x13.ServeClientRequest\x1a\x11.ServeClientReply\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'raft_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_APPENDENTRIESREQUEST']._serialized_start=15
  _globals['_APPENDENTRIESREQUEST']._serialized_end=185
  _globals['_APPENDENTRIESREPLY']._serialized_start=187
  _globals['_APPENDENTRIESREPLY']._serialized_end=289
  _globals['_REQUESTVOTEREQUEST']._serialized_start=292
  _globals['_REQUESTVOTEREQUEST']._serialized_end=422
  _globals['_REQUESTVOTEREPLY']._serialized_start=424
  _globals['_REQUESTVOTEREPLY']._serialized_end=477
  _globals['_LOGENTRY']._serialized_start=479
  _globals['_LOGENTRY']._serialized_end=522
  _globals['_SERVECLIENTREQUEST']._serialized_start=524
  _globals['_SERVECLIENTREQUEST']._serialized_end=561
  _globals['_SERVECLIENTREPLY']._serialized_start=563
  _globals['_SERVECLIENTREPLY']._serialized_end=630
  _globals['_RAFTNODE']._serialized_start=633
  _globals['_RAFTNODE']._serialized_end=820
# @@protoc_insertion_point(module_scope)