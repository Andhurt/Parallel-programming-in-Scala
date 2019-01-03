package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {

    def balanceAux (chars:List[Char], acc: Int): Boolean = {
      if(acc < 0) false
      else
      if(chars.isEmpty) acc == 0
      else balanceAux(chars.tail, acc + calculate(chars.head))
    }

    def calculate (char: Char): Int = {
      if(char=='(') 1
      else
      if (char==')') -1
      else  0
    }

    balanceAux(chars.toList,0)

  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      def traverseAux(chars: Array[Char], acc1:Int, acc2:Int): (Int, Int) = {
        if (chars.isEmpty) (acc1,acc2)
        else if (chars.head == '(') traverseAux(chars.tail, acc1 + 1, acc2)
        else if (chars.head == ')') {
          if (acc1 > 0) traverseAux(chars.tail, acc1 - 1, acc2)
          else traverseAux(chars.tail, acc1, acc2 + 1)
        }
        else traverseAux(chars.tail, acc1, acc2)
      }
      traverseAux(chars.slice(idx, until), arg1, arg2)
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      if(until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = from + (until - from) / 2
        val ((balanceLeft, signLeft), (balanceRight, signRight)) = parallel(
          reduce(from, mid),
          reduce(mid, until)
        )
        if(signRight <= balanceLeft) (balanceLeft-signRight+balanceRight, signLeft)
        else (balanceRight, signLeft+signRight-balanceLeft)
      }

    }

    reduce(0, chars.length) == (0,0)

  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
