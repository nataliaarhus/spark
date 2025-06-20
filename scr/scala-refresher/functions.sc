// Functions
// Format is def <function name>(parameter name: type...) : return type = { expression }
def squareIt(x: Int): Int = {
  x * x
}
def cubeIt(x: Int): Int = {
  x * x * x
}
println(squareIt(2))
println(cubeIt(2))

// Functions can take other functions as parameters
def transformInt(x: Int, f: Int => Int): Int = {
  f(x)
}
val result = transformInt(2, cubeIt)
println(result)

// Lambda functions
transformInt(3, x => x * x * x) //> res0: Int = 27
transformInt(10, x => x / 2) //> res1: Int = 5
transformInt(2, x => {
  val y = x * 2; y * y
})

// EXERCISE
// Strings have a built-in .toUpperCase method. For example, "foo".toUpperCase gives you back FOO.
// Write a function that converts a string to upper-case, and use that function of a few test strings.
// Then, do the same thing using a function literal instead of a separate, named function.

def transform(str: String): String = {
  str.toUpperCase
}

transform("testing1")
transform("testing2")
transform("testing3")

def transformString(str: String, functionTesting: String => String): String = {
  functionTesting(str)
}

transformString("testing4", transform)

transformString("testingliteral", x => x.toUpperCase)