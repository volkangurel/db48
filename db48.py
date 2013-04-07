import os
import mmap
import struct
import time
import logging

_TABLE_NUM_REGIONS = 1024
_TABLE_HEADER_SZ = 4096 + _TABLE_NUM_REGIONS
_TABLE_MAGIC_OFF = 0
_TABLE_MAGIC = 0xdb48beef
_TABLE_CSUM_OFF = 4
_TABLE_REGION_SUMMARY_OFF = 4096
_TABLE_EPOCH = 1364768380

_REGION_NUM_FMES = 1024
_REGION_FME_SZ = 4
_REGION_SZ = 64 * 1024
_REGION_HEADER_SZ = _REGION_NUM_FMES * _REGION_FME_SZ
_REGION_USABLE_SZ = _REGION_SZ - _REGION_HEADER_SZ

_TABLE_SZ = _TABLE_HEADER_SZ + _REGION_SZ * _TABLE_NUM_REGIONS

_FLS_MAGIC_OFF = 0
_FLS_MAGIC = 0x0ff537
_FLS_LEN_OFF = 4
_FLS_NEXT_OFF = 6
_FLS_HEADER_SZ = 8

_FIELD_MAGIC_OFF = 0
_FIELD_MAGIC = 0x48
_FIELD_TYPE_OFF = 1
_FIELD_KEY_OFF = 2
_FIELD_TS_OFF = 4
_FIELD_HEADER_SZ = 8

FL_TYPE_INT = 1
FL_TYPE_BYTES = 2


def get_logger(name):
    level = logging.DEBUG if 'DEBUG' in os.environ else logging.INFO
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger = logging.Logger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
_logger = get_logger(__name__)


class Table(object):

    def __init__(self):
        self.initialized = False
        self._fd = None
        self._mmap = None

    def create(self, path):
        assert not self.initialized
        self._fd = os.open(path, os.O_RDWR | os.O_CREAT)
        os.lseek(self._fd, _TABLE_SZ-1, os.SEEK_SET)
        os.write(self._fd, b'\x00')
        self._mmap = mmap.mmap(self._fd, _TABLE_SZ)
        self._mmap[_TABLE_MAGIC_OFF:_TABLE_MAGIC_OFF+4] = struct.pack(">I", _TABLE_MAGIC)
        self._mmap[_TABLE_CSUM_OFF:_TABLE_CSUM_OFF+4] = struct.pack(">I", 0)
        for i in range(_TABLE_NUM_REGIONS):
            self._mmap[i+_TABLE_CSUM_OFF:i+_TABLE_CSUM_OFF+1] = struct.pack("B", 0)
        for i in range(_TABLE_NUM_REGIONS):
            r = Region(self, i)
            r.create()
        self.initialized = True

    def open(self, path):
        assert not self.initialized
        self._fd = os.open(path, os.O_RDWR)
        self._mmap = mmap.mmap(self._fd, _TABLE_SZ)
        magic = struct.unpack(">I", self._mmap[_TABLE_MAGIC_OFF:_TABLE_MAGIC_OFF+4])[0]
        assert magic == _TABLE_MAGIC
        self.initialized = True

    def insert(self, fls):
        assert self.initialized
        rec_addr = self._insert(fls)
        return rec_addr

    def _insert(self, fls):
        space = fls.length()
        region = self._find_region_with_space(space)
        rec_addr = region.insert(fls, space)
        return rec_addr

    def update(self, rec_addr, fls):
        assert self.initialized
        existing_fls, region = self._lookup(rec_addr)
        existing_length = existing_fls.length()
        existing_fls.update(fls)
        if existing_length < existing_fls.length():
            self._delete(rec_addr, region)
            return self._insert(existing_fls)
        else:
            region.update(rec_addr, existing_fls)
            return rec_addr

    def delete(self, rec_addr):
        assert self.initialized
        self._delete(rec_addr)

    def _delete(self, rec_addr, region=None):
        if region is None:
            region = self._find_region_with_rec(rec_addr)
        region.delete(rec_addr)

    def lookup(self, rec_addr):
        assert self.initialized
        fls, _ = self._lookup(rec_addr)
        return fls

    def _lookup(self, rec_addr):
        region = self._find_region_with_rec(rec_addr)
        fls = region.read(rec_addr)
        return fls, region

    def close(self):
        assert self.initialized
        self._mmap.close()
        os.close(self._fd)
        self.initialized = False

    def _find_region_with_space(self, space):
        region_summaries = struct.unpack_from(">" + "B"*_TABLE_NUM_REGIONS, self._mmap[_TABLE_REGION_SUMMARY_OFF:])
        tries = 0
        for ndx in range(_TABLE_NUM_REGIONS):
            percent_full = float(region_summaries[ndx]) / 100.0
            free_space = _REGION_USABLE_SZ * (1.0 - percent_full)
            if free_space >= space and percent_full <= 0.95:
                return Region(self, ndx)
        raise NoSpace()

    def _find_region_with_rec(self, rec_addr):
        ndx = rec_addr // _REGION_USABLE_SZ
        return Region(self, ndx)


class Region(object):

    #
    # Invariants:
    #
    # 1) There are always _REGION_NUM_FMES FME's (free-space-maps) to list extents of
    # free space in the region.
    #
    # 2) "Empty" FMEs have their length == 0 and the empty FMEs always come after
    # the full ones.
    #
    # 3) Full FMEs are sorted in order of increasing offset.
    #

    class FME():
        def __init__(self, o, l):
            self.offset = o
            self.length = l

    def __init__(self, table, ndx):
        self.table = table
        self.offset = _TABLE_HEADER_SZ + ndx*_REGION_SZ
        self.ndx = ndx

    def create(self):
        fme = self.FME(0, _REGION_USABLE_SZ)
        self.table._mmap[self.offset:self.offset+4] = struct.pack(">HH", fme.offset, fme.length)
        # relies on rest of FME array already being zero'd which fills each fme.length with zero
        # which is the sentinel meaning end-of-valid-fmes.

    def _load_fmes(self):
        raw_fmes = struct.unpack(">" + "HH"*_REGION_NUM_FMES, self.table._mmap[self.offset:self.offset+_REGION_HEADER_SZ])
        assert len(raw_fmes) == 2 * _REGION_NUM_FMES
        fmes = [self.FME(raw_fmes[2*i], raw_fmes[2*i+1]) for i in range(_REGION_NUM_FMES)]
        _logger.debug('loaded FMEs %s' % ', '.join('(%d,%d)' % (f.offset, f.length) for f in fmes if f.length > 0))
        return fmes

    def _store_fmes(self, fmes):
        assert len(fmes) == _REGION_NUM_FMES
        raw_fmes = [struct.pack(">HH", fme.offset, fme.length) for fme in fmes]
        self.table._mmap[self.offset:self.offset+_REGION_HEADER_SZ] = b"".join(raw_fmes)

        temp_fmes = self._load_fmes()
        for i in range(len(temp_fmes)):
            assert fmes[i].length == temp_fmes[i].length

    def insert(self, fls, space):
        fmes = self._load_fmes()
        # assert fmes[1].offset == 0
        # assert fmes[1].length == 0
        for i in range(_REGION_NUM_FMES):
            if fmes[i].length == 0: break   # length == 0 means we're at end of valid fmes
            if fmes[i].length < space: continue
            _logger.debug('inserting %d bytes at %d' % (space, fmes[i].offset))
            rec_addr = fmes[i].offset + self.ndx*_REGION_USABLE_SZ
            if fmes[i].length == space:
                assert False
                del fmes[i]
                empty_fme = self.FME(0, 0)
                fmes.append(empty_fme)
            else:
                fmes[i].offset += space
                fmes[i].length -= space
            # assert fmes[i+1].offset == 0
            # assert fmes[i+1].length == 0
            self._store_fmes(fmes)
            rec_offset = rec_addr + (self.ndx + 1) * _REGION_HEADER_SZ + _TABLE_HEADER_SZ
            fls.store(rec_offset, self.table._mmap)
            return rec_addr
        raise NoSpace()

    def update(self, rec_addr, fls):
        rec_off_in_region = rec_addr % _REGION_USABLE_SZ
        offset = self.offset + _REGION_HEADER_SZ + rec_off_in_region
        _logger.debug('updating record at %d' % (rec_off_in_region))
        fls.store(offset, self.table._mmap)

    def delete(self, rec_addr):
        rec_off_in_region = rec_addr % _REGION_USABLE_SZ
        offset = self.offset + _REGION_HEADER_SZ + rec_off_in_region
        rec_len = FieldList.delete(offset, self.table._mmap)
        _logger.debug('deleting %d bytes at %d' % (rec_len, rec_off_in_region))
        self._free_up_space(rec_off_in_region, rec_len)

    def read(self, rec_addr):
        rec_off_in_region = rec_addr % _REGION_USABLE_SZ
        offset = self.offset + _REGION_HEADER_SZ + rec_off_in_region
        fls = FieldList.load(offset, self.table._mmap)
        return fls

    def _free_up_space(self, offset, length):  # offset in region
        _logger.debug('freeing up %d bytes at %d' % (length, offset))
        fmes = self._load_fmes()
        len_fmes = len(fmes)
        new_lower, new_upper = offset, offset + length
        for i in range(len_fmes):
            fme = fmes[i]
            assert fme.length != 0
            fme_lower, fme_upper = fme.offset, fme.offset + fme.length
            if fme_upper < new_lower:
                # this fme is too low, continue
                continue
            elif fme_upper == new_lower:
                # then extend this fme upward
                fme.length += length
                if len_fmes > (i+1):
                    next_fme = fmes[i+2]
                    if next_fme.offset == (fme.offset + fme.length):
                        # then merge with the next one
                        fme.length += next_fme.length
                        del fmes[i+1]
                        fmes.append(self.FME(0, 0))
                break
            # now fme_upper > new_lower
            elif fme_lower < new_upper:
                # this should never happen (overlapping existing FME with new FME)
                raise Exception('cannot free up space within an FME')
            elif fme_lower == new_upper:
                # then extend this fme downward
                fme.offset -= length
                # previous if statement is guaranteed to handle the case where this would touch another fme, so break
                break
            else:  # fme_lower > new_upper:
                # create a new fme here
                assert fmes[-1].length == 0  # TODO clean up fmes if they get too fragmented
                fmes.insert(i, self.FME(offset, length))
                del fmes[-1:]
                break
        _logger.debug('new FMEs %s' % ', '.join('(%d,%d)' % (f.offset, f.length) for f in fmes if f.length > 0))
        self._store_fmes(fmes)


class FieldList(object):
    def __init__(self, fls):
        self.fls = fls

    @staticmethod
    def set(fls):
        assert len(fls) > 0
        fls = list(fls)
        fls.sort(key=lambda x: x.key)
        ts = _get_time()
        for fl in fls:
            fl.ts = ts
        return FieldList(fls)

    def update(self, new_field_list):
        new_fls = new_field_list.fls
        if not new_fls: return
        new_fls.sort(key=lambda x: x.key)
        index = self.index()
        ts = _get_time()
        for new_fl in new_fls:
            new_fl.ts = ts
            fl = index.get(new_fl.key)
            if fl is None:
                self.fls.append(new_fl)
            else:
                fl.update(new_fl)

    def index(self):
        index = {}
        for fl in self.fls:
            index[fl.key] = fl
        return index

    def length(self):
        return _FLS_HEADER_SZ + sum(fl.length() for fl in self.fls)

    def store(self, offset, mmap_):
        raw_fls = b"".join(fl.as_raw() for fl in self.fls)
        raw_header = struct.pack(">IHH", _FLS_MAGIC, len(raw_fls) + _FLS_HEADER_SZ, 0)
        assert len(raw_header) == _FLS_HEADER_SZ
        raw = raw_header + raw_fls
        assert self.length() == len(raw)
        mmap_[offset:offset+len(raw)] = raw

    @staticmethod
    def load(offset, mmap_):
        fls = []
        rec_magic, rec_len, _ = struct.unpack(">IHH", mmap_[offset:offset+8])
        assert rec_magic == _FLS_MAGIC
        if rec_len == 0:
            raise RecordDeleted()
        offset += 8
        rec_len -= 8
        while rec_len > 0:
            fl_len, fl = Field.from_raw(offset, mmap_)
            rec_len -= fl_len
            offset += fl_len
            fls.append(fl)
        assert rec_len == 0
        return FieldList(fls)

    @staticmethod
    def delete(offset, mmap_):
        rec_magic, rec_len, _ = struct.unpack(">IHH", mmap_[offset:offset+8])
        assert rec_magic == _FLS_MAGIC
        raw = struct.pack(">IHH", _FLS_MAGIC, 0, 0)
        mmap_[offset:offset+len(raw)] = raw
        return rec_len


class Field(object):
    __slots__ = ['type', 'key', 'value', 'ts']

    def __init__(self, type_, key, value, ts=None):
        self.type = type_
        self.key = key
        self.value = value
        self.ts = ts

    def length(self):
        length = _FIELD_HEADER_SZ
        if self.type == FL_TYPE_INT:
            length += 4
        elif self.type == FL_TYPE_BYTES:
            length += 2
            length += len(self.value)
        return length

    def as_raw(self):
        out = struct.pack(">BBHI", _FIELD_MAGIC, self.type, self.key, self.ts)
        assert len(out) == _FIELD_HEADER_SZ
        if self.type == FL_TYPE_INT:
            out += struct.pack(">I", self.value)
        elif self.type == FL_TYPE_BYTES:
            out += struct.pack(">H", len(self.value))
            out += self.value
        assert self.length() == len(out)
        return out

    @staticmethod
    def from_raw(offset, mmap_):
        length = _FIELD_HEADER_SZ
        fl_magic, fl_type, fl_key, fl_ts = struct.unpack(">BBHI", mmap_[offset:offset+_FIELD_HEADER_SZ])
        assert fl_magic == _FIELD_MAGIC
        assert fl_type in (FL_TYPE_INT, FL_TYPE_BYTES, )
        offset += _FIELD_HEADER_SZ
        if fl_type == FL_TYPE_INT:
            value = struct.unpack(">I", mmap_[offset:offset+4])[0]
            length += 4
        elif fl_type == FL_TYPE_BYTES:
            value_len = struct.unpack(">H", mmap_[offset:offset+2])[0]
            offset += 2
            length += 2
            value = mmap_[offset:offset+value_len]
            length += value_len
        fl = Field(fl_type, fl_key, value, fl_ts)
        return length, fl

    def update(self, new_fl):
        for k in self.__slots__:
            setattr(self, k, getattr(new_fl, k))


class NoSpace(Exception):
    pass


class RecordDeleted(Exception):
    pass


def _get_time():
    return int((time.time() - _TABLE_EPOCH) * 1000)
