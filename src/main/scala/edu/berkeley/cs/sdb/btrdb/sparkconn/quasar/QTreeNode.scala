package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types.{StatRecord, Block, Coreblock, Vectorblock}
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.blockstore.{PWFACTOR, KFACTOR}
import util.control.Breaks._

/**
 * Created by almightykim on 5/10/15.
 */
class QTreeNode(
  val tr: QTree,
  var vector_block: Vectorblock,
  var core_block: Coreblock,
  var isLeaf: Boolean,
  var parent: QTreeNode,
  var isNew: Boolean) {

  def this(qtree:QTree) = {
    this(qtree, null, null, false, null, true)
  }

  val child_cache:Array[QTreeNode] = new Array[QTreeNode](KFACTOR)

  def ThisAddr() : Long = {
    if (isLeaf) {
      this.vector_block.Identifier
    } else {
      this.core_block.Identifier
    }
  }

  def StartTime() : Long = {
    if (this.isLeaf) {
      return this.vector_block.StartTime
    } else {
      return this.core_block.StartTime
    }
  }

  def PointWidth() : Int = {
    if (this.isLeaf) {
      this.vector_block.PointWidth
    } else {
      this.core_block.PointWidth
    }
  }


  //So this might be the only explanation of how PW really relates to time:
  //If the node is core, the node's PW is the log of the amount of time that
  //each child covers. So a pw of 8 means that each child covers 1<<8 nanoseconds
  //If the node is a vector, the PW represents what the PW would be if it were
  //a core. It does NOT represent the PW of the vector itself.
  def WidthTime() : Long = {
    return 1 << this.PointWidth()
  }


  def ClampVBucket(t:Long, pw:Int) : Long = {

    var rt:Long = 0

    if (!this.isLeaf) {
      println("This is intended for vectors")
      return 0
    }

    if (t < this.StartTime()) {
      rt = this.StartTime()
    }

    rt = t - this.StartTime()

    if (pw > this.parent.PointWidth() ) {
      println("I can't do this dave")
      return 0
    }

    var idx = t >>> pw
    val maxidx = this.parent.WidthTime() >>> pw
    if (idx >= maxidx) {
      idx = maxidx - 1
    }
    idx
  }


  def ClampBucket(t:Long) : Int = {
    if (this.isLeaf) {
      println("Not meant to use this on leaves")
    }

    var rt = t

    if (t < this.StartTime()) {
      rt = this.StartTime()
    }
    rt -= this.StartTime()

    var rv = (rt >> this.PointWidth())
    if (rv >= KFACTOR) {
      rv = (KFACTOR - 1)
    }
    (rv & 0xFFFF).toInt
  }



  def TreePath() : String = {
    var rv = ""
    if ( this.isLeaf) {
      rv += "V"
    } else {
      rv += "C"
    }
/*
    var dn = this
    val found = false
    for {
      val par = this.parent
      if (par == null) {
        return rv
      }
      //Try locate the index of this node in the parent
      val addr = dn.ThisAddr()
      found = false

    for i := 0; i < bstore.KFACTOR; i++ {
      if par.core_block.Addr[i] == addr {
        rv = fmt.Sprintf("(%v)[%v].", par.PointWidth(), i) + rv
        found = true
        break
      }
    }
        if !found {
          log.Panicf("Could not find self address in parent")
        }
        dn = par
    }
*/
    return rv
  }

  def ChildPW() : Int = {
    if (this.PointWidth() <= PWFACTOR) {
      0
    } else {
      (this.PointWidth() - PWFACTOR)
    }
  }

  def ChildStartTime(idx:Int) : Long = {
    this.ArbitraryStartTime(idx, this.PointWidth())
  }

  def ChildEndTime(idx:Int) : Long = {
    this.ArbitraryStartTime((idx+1), this.PointWidth())
  }

  def ArbitraryStartTime(idx:Long, pw:Long) : Long = {
    return this.StartTime() + (idx * (1<<pw))
  }


  def Child(i:Int) : QTreeNode = {
    //log.Debug("Child %v called on %v",i, n.TreePath())
    if (this.isLeaf) {
      println("Child of leaf?")
      return null
    }

    if (this.core_block.Addr(i) == 0) {
      return null
    }

    if (this.child_cache(i) != null) {
      return this.child_cache(i)
    }

    val child = this.tr.LoadNode(this.core_block.Addr(i),  this.core_block.CGeneration(i), this.ChildPW(), this.ChildStartTime(i))

/*

    if err != nil {
      log.Debug("We are at %v", n.TreePath())
      log.Debug("We were trying to load child %v", i)
      log.Debug("With address %v", n.core_block.Addr[i])
      log.Panicf("%v", err)
    }
*/

    child.parent = this
    this.child_cache(i) = child
    return child
  }

  //Although we keep caches of datablocks in the bstore, we can't actually free them until
  //they are unreferenced. This dropcache actually just makes sure they are unreferenced
  def Free() : Unit = {
    if (this.isLeaf) {
      this.vector_block = null
    } else {

      for (i <- 0 until this.child_cache.length) {
        val c:QTreeNode = this.child_cache(i)
        if (c != null) {
          c.Free()
          this.child_cache(i) = null
        }
      }
      this.core_block = null
    }
  }

  /*

  ok so here is the problem. If we call opreduce on a core node, then we can only deliver
  pointwidths GREATER than our pointwidth and less than pointwidth + 6 right?
  but as a leaf we can potentially deliver pointwidths down to 0...
  */
  def OpReduce(pointwidth:Int, index:Long) : (Long, Double, Double, Double) = {
    if (!this.isLeaf && pointwidth < this.PointWidth()) {
      println("Bad pointwidth for core. See code comment")
      return (0, 0, 0, 0)
    }

    if (pointwidth > this.PointWidth() + PWFACTOR) {
      println("Can't guarantee this PW")
      return (0, 0, 0, 0)
    }

    val maxpw = this.PointWidth() + PWFACTOR
    val pwdelta = pointwidth - this.PointWidth()
    val width = 1 << pointwidth
    val maxidx = 1 << (maxpw - pointwidth)
    if (maxidx <= 0 || index >= maxidx) {
      println("node is %s", this.TreePath())
      println("bad index", maxidx, index)
      return (0, 0, 0, 0)
    }
    var sum = 0.0
    var min = Double.MaxValue
    var max = Double.MinValue
    var minset = false
    var maxset = false
    var count:Long = 0


    if (this.isLeaf) {
      val st = this.StartTime() + (index * width)
      val et = st + width
      if (this.vector_block.Len != 0) {
        breakable(for (i <- 0 until this.vector_block.Len) {

          if (this.vector_block.Time(i) >= et) {
            break
          }

          if (this.vector_block.Time(i) > st) {
            var v = this.vector_block.Value(i)
            sum += v
            if (!minset || v < min) {
              minset = true
              min = v
            }
            if (!maxset || v > max) {
              maxset = true
              max = v
            }
            count += 1
          }
        })
      }
      return (count, min, (sum.toDouble / count.toDouble).toDouble, max)
    } else {
      val s = index << pwdelta
      val e = (index + 1) << pwdelta

      for (i <- s until e) {
        if (this.core_block.Count(i.toInt) != 0) {
          count += this.core_block.Count(i.toInt)
          sum += this.core_block.Mean(i.toInt) * this.core_block.Count(i.toInt).toDouble
          if (!minset || this.core_block.Min(i.toInt) < min) {
            minset = true
            min = this.core_block.Min(i.toInt)
          }
          if (!maxset || this.core_block.Max(i.toInt) > max) {
            maxset = true
            max = this.core_block.Max(i.toInt)
          }

        }
      }
      return (count, min, (sum.toDouble / count.toDouble).toDouble, max)
    }
  }


  def QueryStatisticalValues(rv:StatRecord, start:Long, end:Long, pw:Int) : Unit = {

    if (this.isLeaf) {
      var idx:Long = 0
      breakable(while (idx < this.vector_block.Len) {

        if (this.vector_block.Time(idx.toInt) >= end) {
          break
        }

        if (this.vector_block.Time(idx.toInt) > start) {
          val b = this.ClampVBucket(this.vector_block.Time(idx.toInt), pw)
          val (count, min, mean, max) = this.OpReduce(pw, b)
          if (count != 0)  {
            val rv = new StatRecord(ArbitraryStartTime(b, pw), count, min, mean, max)

            //Skip over records in the vector that the PW included
            idx += (count - 1)
          }else{
            idx += 1
          }
        } else {
          idx += 1
        }
      })

    } else {

      //Ok we are at the correct level and we are a core
      val sb = this.ClampBucket(start) //TODO check this function handles out of range
      val eb = this.ClampBucket(end)

      if (pw <= this.PointWidth()) {
        for (b <- sb until eb) {
          val c = this.Child(b)
          if (c != null) {
            c.QueryStatisticalValues(rv, start, end, pw)
            c.Free()
            this.child_cache(b) = null
          }
        }

      } else {
        val pwdelta = pw - this.PointWidth()
        val sidx = sb >> pwdelta
        val eidx = eb >> pwdelta
        for (b <- sidx until eidx) {
          val (count, min, mean, max) = this.OpReduce(pw, b)
          if (count != 0) {
            val rv = new StatRecord(ArbitraryStartTime(b, pw), count, min, mean, max)
          }
        }
      }
    }
  }

}
