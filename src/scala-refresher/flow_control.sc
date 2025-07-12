// If / else syntax
if (1 > 3) println("Impossible!") else println("The world makes sense.")

if (1 > 3) {
  println("Impossible!")
} else {
  println("The world makes sense.")
}

// Matching - like switch in other languages:
val number = 3
number match {
  case 1 => println("One")
  case 2 => println("Two")
  case 3 => println("Three")
  case _ => println("Something else")
}

// For loops
for (x <- 1 to 4) { // iterate through 1 to 4
  val squared = x * x
  println(squared)
}

// While loops
var x = 10 // mutable
while (x >= 0) {
  println(x)
  x -= 1
}

// Do While Loop
x = 0
do { println(x); x+=1 } while (x <= 10)

// Expressions
{val x = 10; x + 20} // implicitly outputs the last thing in the expression, i.e. 30. It can be treated as it's own entity.

println({val x = 10; x + 20})


// EXERCISE
// Write some code that prints out the first 10 values of the Fibonacci sequence.
// This is the sequence where every number is the sum of the two numbers before it.
// So, the result should be 0, 1, 1, 2, 3, 5, 8, 13, 21, 34

var value = 1
var prevValue = 0
var result = 0
var i = 0

do {
  println(result)
  result = value + prevValue
  prevValue = value
  value = result
  i += 1
} while (i < 10)