import unittest
import os
import struct

import db48


class TestCreateClose(unittest.TestCase):
    path = "/tmp/t.db48"

    def setUp(self):
        try:
            os.unlink(self.path)
        except OSError:
            pass

    def _create(self):
        t = db48.Table()
        t.create(self.path)
        return t

    def _open(self):
        t = self._create()
        t.close()
        t = db48.Table()
        t.open(self.path)
        return t

    def test_create_and_close(self):
        t = self._create()
        r = db48.Region(t, 0)
        fmes = r._load_fmes()
        self.assertTrue(fmes[0].offset == 0)
        self.assertTrue(fmes[0].length == db48._REGION_USABLE_SZ)
        self.assertTrue(fmes[1].offset == 0)
        self.assertTrue(fmes[1].length == 0)
        t.close()

    def test_open(self):
        t = self._open()
        t.close()

    def test_find_region_with_space1(self):
        t = self._open()
        region = t._find_region_with_space(78)
        self.assertTrue(region.offset == db48._TABLE_HEADER_SZ)  # first region
        self.assertRaises(db48.NoSpace, t._find_region_with_space, 64 * 1024 + 1)  # too big

    def test_insert(self):
        t = self._open()
        r = db48.Region(t, 0)
        fmes = r._load_fmes()
        self.assertTrue(fmes[0].offset == 0)
        self.assertTrue(fmes[0].length == db48._REGION_USABLE_SZ)
        self.assertTrue(fmes[1].offset == 0)
        self.assertTrue(fmes[1].length == 0)
        msg = "Hello, World!".encode("utf-8")
        fl1 = db48.Field(db48.FL_TYPE_INT, 0, 42)
        fl2 = db48.Field(db48.FL_TYPE_BYTES, 1, msg)
        fls = db48.FieldList.set((fl1, fl2))
        space = fls.length()
        self.assertTrue(space == (db48._FIELD_HEADER_SZ*2 + db48._FLS_HEADER_SZ + 4 + 2 + len(msg)))
        rid = t.insert(fls)
        self.assertTrue(rid == 0)
        r = db48.Region(t, 0)
        fmes = r._load_fmes()
        self.assertTrue(fmes[0].offset == space)
        self.assertTrue(fmes[0].length == db48._REGION_USABLE_SZ - space)
        self.assertTrue(fmes[1].length == 0)
        t.close()

    def test_lookup(self):
        t = self._open()
        msg = "Hello, World!".encode("utf-8")
        fl1 = db48.Field(db48.FL_TYPE_INT, 0, 42)
        fl2 = db48.Field(db48.FL_TYPE_BYTES, 13, msg)
        fls = db48.FieldList.set((fl1, fl2))
        rid = t.insert(fls)
        fls = t.lookup(rid)
        self.assertTrue(fls.fls[0].value == 42)
        self.assertTrue(fls.fls[0].key == 0)
        self.assertTrue(fls.fls[1].value == msg)
        self.assertTrue(fls.fls[1].key == 13)
        t.close()

    def test_insert2(self):
        t = self._open()
        total_space = 0
        total_num_records = 3
        records = []
        for i in range(total_num_records):
            fl1 = db48.Field(db48.FL_TYPE_INT, 0, i)
            fl2 = db48.Field(db48.FL_TYPE_BYTES, 1, ("Hello %d" % i).encode())
            fls = db48.FieldList.set((fl1, fl2))
            space = fls.length()
            rid = t.insert(fls)
            self.assertTrue(rid == total_space)
            total_space += space
            records.append(rid)
        r = db48.Region(t, 0)
        fmes = r._load_fmes()
        self.assertTrue(fmes[0].offset == total_space)
        self.assertTrue(fmes[0].length == db48._REGION_USABLE_SZ - total_space)
        self.assertTrue(fmes[1].length == 0)
        offset = r.offset + db48._REGION_HEADER_SZ
        for i in range(total_num_records):
            rec_magic, rec_len, _ = struct.unpack(">IHH", r.table._mmap[offset:offset+8])
            assert rec_magic == db48._FLS_MAGIC
            assert rec_len <= db48._FLS_HEADER_SZ + 2*db48._FIELD_HEADER_SZ + 4 + 2 + 9
            assert rec_len >= db48._FLS_HEADER_SZ + 2*db48._FIELD_HEADER_SZ + 4 + 2 + 1
            offset += rec_len
        for i in range(total_num_records-1, -1, -1):
            fls = t.lookup(records[i])
            self.assertTrue(fls.fls[0].key == 0)
            self.assertTrue(fls.fls[0].value == i)
            self.assertTrue(fls.fls[1].key == 1)
            self.assertTrue(fls.fls[1].value.decode() == "Hello %d" % i)
        t.close()

    def test_update(self):
        t = self._open()
        total_num_records = 3
        records = []
        for i in range(total_num_records):
            fl1 = db48.Field(db48.FL_TYPE_INT, 0, i)
            fl2 = db48.Field(db48.FL_TYPE_BYTES, 1, ("Hello %d" % i).encode())
            fls = db48.FieldList.set((fl1, fl2))
            rid = t.insert(fls)
            records.append(rid)
        for i in range(total_num_records-1, -1, -1):
            fls = t.lookup(records[i])
            self.assertTrue(fls.fls[0].key == 0)
            self.assertTrue(fls.fls[0].value == i)
            self.assertTrue(fls.fls[1].key == 1)
            self.assertTrue(fls.fls[1].value.decode() == "Hello %d" % i)
        for i in range(total_num_records):
            new_fl = db48.Field(db48.FL_TYPE_BYTES, 1, ("Hello %d" % (i + 100000)).encode())
            new_fls = db48.FieldList.set((new_fl,))
            records[i] = t.update(records[i], new_fls)
        for i in range(total_num_records-1, -1, -1):
            fls = t.lookup(records[i])
            self.assertTrue(fls.fls[0].key == 0)
            self.assertTrue(fls.fls[0].value == i)
            self.assertTrue(fls.fls[1].key == 1)
            self.assertTrue(fls.fls[1].value.decode() == "Hello %d" % (i + 100000))
        t.close()

    def test_delete(self):
        t = self._open()
        total_num_records = 3
        records = []
        for i in range(total_num_records):
            fl1 = db48.Field(db48.FL_TYPE_INT, 0, i)
            fl2 = db48.Field(db48.FL_TYPE_BYTES, 1, ("Hello %d" % i).encode())
            fls = db48.FieldList.set((fl1, fl2))
            rid = t.insert(fls)
            records.append(rid)
        for i in range(total_num_records-1, -1, -1):
            fls = t.lookup(records[i])
            self.assertTrue(fls.fls[0].key == 0)
            self.assertTrue(fls.fls[0].value == i)
            self.assertTrue(fls.fls[1].key == 1)
            self.assertTrue(fls.fls[1].value.decode() == "Hello %d" % i)
        for rid in records:
            t.delete(rid)
        for i in range(total_num_records-1, -1, -1):
            self.assertRaises(db48.RecordDeleted, t.lookup, records[i])
        t.close()

unittest.main()
