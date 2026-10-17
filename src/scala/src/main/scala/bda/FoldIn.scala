package bda

/** Pure-Scala ALS "fold-in": compute a user/session vector from a few interactions without retraining.
  *
  * For implicit-feedback ALS (Hu, Koren, Volinsky formulation as implemented in Spark MLlib), with fixed item factors Y,
  * preferences p_i = 1 and confidences c_i = 1 + alpha * r_i, the optimal user vector solves
  *     (YtY + sum_{i in S} (c_i - 1) y_i y_i^T + lambda * |S| * I) x = sum_{i in S} c_i y_i
  * YtY is computed once and broadcast, so each fold-in costs O(|S| * rank^2 + rank^3): microseconds for rank 32.
  * Written with immutable inputs, higher-order helpers and a small ADT for the weighting so it can be unit tested without Spark.
  */
object FoldIn {
  /** Interaction weights r_i used as implicit feedback strength. */
  sealed trait EventKind { def weight: Double }
  case object View extends EventKind { val weight = 1.0 }
  case object Cart extends EventKind { val weight = 3.0 }
  case object Purchase extends EventKind { val weight = 5.0 }

  def kindOf(eventType: String): Option[EventKind] = eventType match {   // pattern matching (CO1)
    case "view" => Some(View)
    case "cart" => Some(Cart)
    case "purchase" => Some(Purchase)
    case _ => None
  }

  /** Item factor table: dense, row-major, so it broadcasts cheaply. index -> item id is `ids`. */
  final case class ItemFactors(ids: Array[Long], flat: Array[Float], rank: Int) {
    val index: Map[Long, Int] = ids.zipWithIndex.toMap
    def row(i: Int): Array[Float] = java.util.Arrays.copyOfRange(flat, i * rank, (i + 1) * rank)
    /** Y^T Y as rank x rank matrix. */
    lazy val gram: Array[Array[Double]] = {
      val g = Array.ofDim[Double](rank, rank)
      var n = 0
      while (n < ids.length) {
        val off = n * rank; var a = 0
        while (a < rank) { val va = flat(off + a).toDouble; var b = 0; while (b < rank) { g(a)(b) += va * flat(off + b); b += 1 }; a += 1 }
        n += 1
      }
      g
    }
  }

  /** Solve A x = b for symmetric positive definite A by Cholesky; A is modified copy-free (works on local copies). */
  def solveSpd(a: Array[Array[Double]], b: Array[Double]): Array[Double] = {
    val n = b.length
    val l = Array.ofDim[Double](n, n)
    for (i <- 0 until n; j <- 0 to i) {
      var s = a(i)(j)
      var k = 0; while (k < j) { s -= l(i)(k) * l(j)(k); k += 1 }
      if (i == j) l(i)(i) = math.sqrt(math.max(s, 1e-12)) else l(i)(j) = s / l(j)(j)
    }
    val y = new Array[Double](n)
    for (i <- 0 until n) { var s = b(i); var k = 0; while (k < i) { s -= l(i)(k) * y(k); k += 1 }; y(i) = s / l(i)(i) }
    val x = new Array[Double](n)
    for (i <- n - 1 to 0 by -1) { var s = y(i); var k = i + 1; while (k < n) { s -= l(k)(i) * x(k); k += 1 }; x(i) = s / l(i)(i) }
    x
  }

  /** Session vector from (itemId, weight) pairs; None if no item is known to the model. */
  def sessionVector(f: ItemFactors, session: Seq[(Long, Double)], lambda: Double, alpha: Double): Option[Array[Double]] = {
    val known = session.flatMap { case (id, r) => f.index.get(id).map(_ -> r) }
    if (known.isEmpty) None
    else {
      val r = f.rank
      val a = Array.tabulate(r, r)((i, j) => f.gram(i)(j) + (if (i == j) lambda * known.size else 0.0))
      val b = new Array[Double](r)
      known.foreach { case (idx, w) =>
        val y = f.row(idx); val c = 1.0 + alpha * w
        var i = 0
        while (i < r) {
          b(i) += c * y(i)
          var j = 0; while (j < r) { a(i)(j) += (c - 1.0) * y(i) * y(j); j += 1 }
          i += 1
        }
      }
      Some(solveSpd(a, b))
    }
  }

  /** Top-n item ids by dot product with `x`, skipping `exclude`. Uses a bounded min-heap (no full sort). */
  def topN(f: ItemFactors, x: Array[Double], n: Int, exclude: Set[Long]): Array[Long] = {
    val heap = new java.util.PriorityQueue[(Double, Long)](n + 1, Ordering.by[(Double, Long), Double](_._1))
    var i = 0
    while (i < f.ids.length) {
      val id = f.ids(i)
      if (!exclude.contains(id)) {
        var s = 0.0; val off = i * f.rank; var k = 0
        while (k < f.rank) { s += x(k) * f.flat(off + k); k += 1 }
        if (heap.size < n) heap.add((s, id)) else if (s > heap.peek()._1) { heap.poll(); heap.add((s, id)) }
      }
      i += 1
    }
    val out = new Array[(Double, Long)](heap.size); var j = out.length - 1
    while (!heap.isEmpty) { out(j) = heap.poll(); j -= 1 }
    out.map(_._2)
  }
}
