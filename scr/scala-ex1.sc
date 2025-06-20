// VALUES are immutable constants.

val hello: String = "Hello"

// VARIABLES are mutable
var helloThere: String = hello
helloThere = hello + " There!"
println(helloThere)

// DATA TYPES
val numberOne: Int = 1
val truth: Boolean = true
val letterA: Char = 'a'
val pi: Double = 3.14159256
val piSinglePrecision: Float = 3.14159256f
val bigNumber: Long = 123456789
val smallNumber: Byte = 127

println("Concatenated text: " + hello + helloThere)
println(f"Pi is about $piSinglePrecision%.3f")
println(s"Use s prefix to substitute values like $numberOne or $truth")
println(s"${1+2}")

val theUltimateAnswer: String = "To live, the universe and everything is 42."
val pattern = """.* ([\d]+).*""".r // regular expression
val pattern(answerString) = theUltimateAnswer
val answer = answerString.toInt
println(answer)