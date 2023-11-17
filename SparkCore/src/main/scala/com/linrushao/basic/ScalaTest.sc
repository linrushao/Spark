val arr = (6,-2,5,9,3,8,3,7,7,2)

var count =0
def getFib(n: Int): Int = {
  count += 1
  if (n == 1 || n == 2) {
    1
  } else {
    getFib(n - 1) + getFib(n - 2)
  }
}
val list = List(List(1, 2), List(3, 4))
val ints = list.flatMap(x => x).map(_+7)


//单词计数
val strings = List("hello tom","hello jerry", "hello kitty")
val stringses = strings.flatMap(_.split(" "))
stringses.groupBy(x=>x)