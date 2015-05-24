/*
	The purpose of this file is to illustrate how BTrDB re-constructs its tree from stroage.
*/

const PWFACTOR = bstore.PWFACTOR
const KFACTOR = bstore.KFACTOR
const MICROSECOND = 1000
const MILLISECOND = 1000 * MICROSECOND
const SECOND = 1000 * MILLISECOND
const MINUTE = 60 * SECOND
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR
const ROOTPW = 56 //This makes each bucket at the root ~= 2.2 years
//so the root spans 146.23 years
const ROOTSTART = -1152921504606846976 //This makes the 16th bucket start at 1970 (0)
const MinimumTime = -(16 << 56)
const MaximumTime = (48 << 56)



	// The leaf datablock type. The tags allow unit tests
	// to work out if clone / serdes are working properly
	// metadata is not copied when a node is cloned
	// implicit is not serialised
	type Vectorblock struct {

		//Metadata, not copied on clone
		Identifier uint64 "metadata,implicit"
		Generation uint64 "metadata,implicit"

		//Payload, copied on clone
		Len        uint16
		PointWidth uint8 "implicit"
		StartTime  int64 "implicit"
		Time       [VSIZE]int64
		Value      [VSIZE]float64
	}

	type Coreblock struct {

		//Metadata, not copied
		Identifier uint64 "metadata,implicit"
		Generation uint64 "metadata,implicit"

		//Payload, copied
		PointWidth  uint8 "implicit"
		StartTime   int64 "implicit"
		Addr        [KFACTOR]uint64
		Count       [KFACTOR]uint64
		Min         [KFACTOR]float64
		Mean        [KFACTOR]float64
		Max         [KFACTOR]float64
		CGeneration [KFACTOR]uint64
	}


	type Superblock struct {
		uuid     uuid.UUID
		gen      uint64
		root     uint64
		unlinked bool
	}

	type QTree struct {
		sb       *bstore.Superblock
		bs       *bstore.BlockStore
		gen      *bstore.Generation
		root     *QTreeNode
		commited bool
	}

	type Record struct {
		Time int64
		Val  float64
	}

	type QTreeNode struct {
		tr           *QTree
		vector_block *bstore.Vectorblock
		core_block   *bstore.Coreblock
		isLeaf       bool
		child_cache  [bstore.KFACTOR]*QTreeNode
		parent       *QTreeNode
		isNew        bool
	}

	type Quasar struct {
		cfg QuasarConfig
		bs  *bstore.BlockStore

		//Transaction coalescence
		globlock  sync.Mutex
		treelocks map[[16]byte]*sync.Mutex
		openTrees map[[16]byte]*openTree
	}






q.QueryStatisticalValues(id, st, et, version, pw)

func (q *Quasar) QueryStatisticalValues(id uuid.UUID, start int64, end int64, gen uint64, pointwidth uint8) ([]qtree.StatRecord, uint64, error) 
{
	start &^= ((1<<pointwidth)-1)
	end &^= ((1<<pointwidth)-1)
    end -= 1
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, 0, err
	}
	rv, err := tr.QueryStatisticalValuesBlock(start, end, pointwidth)
	if err != nil {
		return nil, 0, err
	}
	return rv, tr.Generation(), nil
}

	func NewReadQTree(bs *bstore.BlockStore, id uuid.UUID, generation uint64) (*QTree, error) 
	{
		sb := bs.LoadSuperblock(id, generation)
		if sb == nil {
			return nil, ErrNoSuchStream
		}
		rv := &QTree{sb: sb, bs: bs}
		if sb.Root() != 0 {
			rt, err := rv.LoadNode(sb.Root(), sb.Gen(), ROOTPW, ROOTSTART)
			if err != nil {
				log.Panicf("%v", err)
				return nil, err
			}
			//log.Debug("The start time for the root is %v",rt.StartTime())
			rv.root = rt
		}
		return rv, nil
	}

		func (bs *BlockStore) LoadSuperblock(id uuid.UUID, generation uint64) *Superblock 
		{
			var sb = fake_sblock{}
			if generation == LatestGeneration {
				//log.Info("loading superblock uuid=%v (lgen)", id.String())
				qry := bs.db.C("superblocks").Find(bson.M{"uuid": id.String()})
				if err := qry.Sort("-gen").One(&sb); err != nil {
					if err == mgo.ErrNotFound {
						log.Info("sb notfound!")
						return nil
					} else {
						log.Panic(err)
					}
				}
			} else {
				qry := bs.db.C("superblocks").Find(bson.M{"uuid": id.String(), "gen": generation})
				if err := qry.One(&sb); err != nil {
					if err == mgo.ErrNotFound {
						return nil
					} else {
						log.Panic(err)
					}
				}
			}
			rv := Superblock{
				uuid:     id,
				gen:      sb.Gen,
				root:     sb.Root,
				unlinked: sb.Unlinked,
			}
			return &rv
		}

		func (tr *QTree) LoadNode(addr uint64, impl_Generation uint64, impl_Pointwidth uint8, impl_StartTime int64) (*QTreeNode, error) 
		{
			db := tr.bs.ReadDatablock(tr.sb.Uuid(), addr, impl_Generation, impl_Pointwidth, impl_StartTime)
			
			n := &QTreeNode{tr: tr}


			switch db.GetDatablockType() {
			case bstore.Vector:
				n.vector_block = db.(*bstore.Vectorblock)
				n.isLeaf = true
			case bstore.Core:
				n.core_block = db.(*bstore.Coreblock)
				n.isLeaf = false
			default:
				log.Panicf("What kind of type is this? %+v", db.GetDatablockType())
			}
			if n.ThisAddr() == 0 {
				log.Panicf("Node has zero address")
			}
			return n, nil
		}

			func (bs *BlockStore) ReadDatablock(uuid uuid.UUID, addr uint64, impl_Generation uint64, impl_Pointwidth uint8, impl_StartTime int64) Datablock 
			{
				//Try hit the cache first
				db := bs.cacheGet(addr)
				if db != nil {
					return db
				}
				syncbuf := block_buf_pool.Get().([]byte)


				/************************************************************ the heart of everything ************************************************************/
				trimbuf := bs.store.Read([]byte(uuid), addr, syncbuf)

				switch DatablockGetBufferType(trimbuf) {
				case Core:
					rv := &Coreblock{}
					rv.Deserialize(trimbuf)
					block_buf_pool.Put(syncbuf)
					rv.Identifier = addr
					rv.Generation = impl_Generation
					rv.PointWidth = impl_Pointwidth
					rv.StartTime = impl_StartTime
					bs.cachePut(addr, rv)
					return rv
				case Vector:
					rv := &Vectorblock{}
					rv.Deserialize(trimbuf)
					block_buf_pool.Put(syncbuf)
					rv.Identifier = addr
					rv.Generation = impl_Generation
					rv.PointWidth = impl_Pointwidth
					rv.StartTime = impl_StartTime
					bs.cachePut(addr, rv)
					return rv
				}
				log.Panic("Strange datablock type")
				return nil
			}

				type StorageProvider interface 
				{
					// Read the blob into the given buffer
					Read(uuid []byte, address uint64, buffer []byte) []byte
				}


				func (sp *CephStorageProvider) Read(uuid []byte, address uint64, buffer []byte) []byte {
					//Get the first chunk for this object:
					chunk1 := sp.obtainChunk(uuid, address&R_ADDRMASK)[address&R_OFFSETMASK:]
					var chunk2 []byte
					var ln int

					if len(chunk1) < 2 {
						//not even long enough for the prefix, must be one byte in the first chunk, one in teh second
						chunk2 = sp.obtainChunk(uuid, (address+R_CHUNKSIZE)&R_ADDRMASK)
						ln = int(chunk1[0]) + (int(chunk2[0]) << 8)
						chunk2 = chunk2[1:]
						chunk1 = chunk1[1:]
					} else {
						ln = int(chunk1[0]) + (int(chunk1[1]) << 8)
						chunk1 = chunk1[2:]
					}

					if (ln) > MAX_EXPECTED_OBJECT_SIZE {
						log.Panic("WTUF: ", ln)
					}

					copied := 0
					if len(chunk1) > 0 {
						//We need some bytes from chunk1
						end := ln
						if len(chunk1) < ln {
							end = len(chunk1)
						}
						copied = copy(buffer, chunk1[:end])
					}
					if copied < ln {
						//We need some bytes from chunk2
						if chunk2 == nil {
							chunk2 = sp.obtainChunk(uuid, (address+R_CHUNKSIZE)&R_ADDRMASK)
						}
						copy(buffer[copied:], chunk2[:ln-copied])

					}
					if ln < 2 {
						log.Panic("This is unexpected")
					}
					return buffer[:ln]

				}



				func (sp *CephStorageProvider) obtainChunk(uuid []byte, address uint64) []byte {
					chunk := sp.rcache.cacheGet(address)
					if chunk == nil {
						chunk = sp.rcache.getBlank()
						rhidx := <-sp.rhidx
						rc, err := C.handle_read(sp.rh[rhidx], (*C.uint8_t)(unsafe.Pointer(&uuid[0])), C.uint64_t(address), (*C.char)(unsafe.Pointer(&chunk[0])), R_CHUNKSIZE)
						if err != nil {
							log.Panic("CGO ERROR: %v", err)
						}
						chunk = chunk[0:rc]
						sp.rhidx_ret <- rhidx
						sp.rcache.cachePut(address, chunk)
					}
					return chunk
				}


				void make_object_id(uint8_t *uuid, uint64_t address, char* dest)
				{
					int i;
					int dp;
					for (i=0;i<16;i++)
					{
						int nibble;
						dest[i*2] 	= nibbles[uuid[i]>>4];
						dest[i*2+1] = nibbles[uuid[i]&0xF];
					}
					for (i=0;i<10;i++)
					{
						dest[32+i] = nibbles[address >> (4*(9-i)) & 0xF];
					}
					dest[OID_SIZE-1] = 0;
				}


				int handle_read(cephprovider_handle_t *h, uint8_t *uuid, uint64_t address, char* dest, int len)
				{
					//The ceph provider uses 24 bits of address per object, and the top 40 bits as an object ID
					int offset = address & 0xFFFFFF;
					uint64_t id = address >> 24;
					int rv;
					char oid [OID_SIZE];
					make_object_id(uuid, id, &oid[0]);
					rv = rados_read(h->ctx, oid, dest, len, offset);
					if (rv < 0)
					{	
						fprintf(stderr, "could not read %s\n", oid);
						errno = -rv;
						return -1;
					}
					errno = 0;
					return rv;
				}

				/**
				 * Read data from an object
				 *
				 * The io context determines the snapshot to read from, if any was set
				 * by rados_ioctx_snap_set_read().
				 *
				 * @param io the context in which to perform the read
				 * @param oid the name of the object to read from
				 * @param buf where to store the results
				 * @param len the number of bytes to read
				 * @param off the offset to start reading from in the object
				 * @returns number of bytes read on success, negative error code on
				 * failure
				 */
				CEPH_RADOS_API int rados_read(rados_ioctx_t io, const char *oid, char *buf, size_t len, uint64_t off);


	func (tr *QTree) QueryStatisticalValuesBlock(start int64, end int64, pw uint8) ([]StatRecord, error) 
	{
		rv := make([]StatRecord, 0, 256)
		recordc := make(chan StatRecord)
		errc := make(chan error)
		var err error
		busy := true
		go tr.QueryStatisticalValues(recordc, errc, start, end, pw)
		for busy {
			select {
			case e, _ := <-errc:
				if e != nil {
					err = e
					busy = false
				}
			case r, r_ok := <-recordc:
				if !r_ok {
					busy = false
				} else {
					rv = append(rv, r)
				}
			}
		}
		return rv, err
	}

		func (tr *QTree) QueryStatisticalValues(rv chan StatRecord, err chan error, start int64, end int64, pw uint8) 
		{
			//Remember end is inclusive for QSV
			if tr.root != nil {
				tr.root.QueryStatisticalValues(rv, err, start, end, pw)
			}
			close(rv)
			close(err)
		}

			func (n *QTreeNode) QueryStatisticalValues(rv chan StatRecord, err chan error, start int64, end int64, pw uint8) 
			{
				if n.isLeaf {
					for idx := 0; idx < int(n.vector_block.Len); idx++ {
						if n.vector_block.Time[idx] < start {
							continue
						}
						if n.vector_block.Time[idx] >= end {
							break
						}
						b := n.ClampVBucket(n.vector_block.Time[idx], pw)
						count, min, mean, max := n.OpReduce(pw, uint64(b))
						if count != 0 {
							rv <- StatRecord{Time: n.ArbitraryStartTime(b, pw),
								Count: count,
								Min:   min,
								Mean:  mean,
								Max:   max,
							}
							//Skip over records in the vector that the PW included
							idx += int(count - 1)
						}
					}
				} else {
					//Ok we are at the correct level and we are a core
					sb := n.ClampBucket(start) //TODO check this function handles out of range
					eb := n.ClampBucket(end)
					recurse := pw <= n.PointWidth()
					if recurse {
						for b := sb; b <= eb; b++ {
							c := n.Child(b)
							if c != nil {
								c.QueryStatisticalValues(rv, err, start, end, pw)
								c.Free()
								n.child_cache[b] = nil
							}
						}
					} else {
						pwdelta := pw - n.PointWidth()
						sidx := sb >> pwdelta
						eidx := eb >> pwdelta
						for b := sidx; b <= eidx; b++ {
							count, min, mean, max := n.OpReduce(pw, uint64(b))
							if count != 0 {
								rv <- StatRecord{Time: n.ChildStartTime(b << pwdelta),
									Count: count,
									Min:   min,
									Mean:  mean,
									Max:   max,
								}
							}
						}
					}
				}
			}


				//Unlike core nodes, vectors have infinitely many buckets. This
				//function allows you to get a bucket idx for a time and an
				//arbitrary point width
				func (n *QTreeNode) ClampVBucket(t int64, pw uint8) uint64 {
					if !n.isLeaf {
						log.Panicf("This is intended for vectors")
					}
					if t < n.StartTime() {
						t = n.StartTime()
					}
					t -= n.StartTime()
					if pw > n.Parent().PointWidth() {
						log.Panicf("I can't do this dave")
					}
					idx := uint64(t) >> pw
					maxidx := uint64(n.Parent().WidthTime()) >> pw
					if idx >= maxidx {
						idx = maxidx - 1
					}
					return idx
				}

				func (n *QTreeNode) ClampBucket(t int64) uint16 
				{
					if n.isLeaf {
						log.Panicf("Not meant to use this on leaves")
					}
					if t < n.StartTime() {
						t = n.StartTime()
					}
					t -= n.StartTime()

					rv := (t >> n.PointWidth())
					if rv >= bstore.KFACTOR {
						rv = bstore.KFACTOR - 1
					}
					return uint16(rv)
				}

