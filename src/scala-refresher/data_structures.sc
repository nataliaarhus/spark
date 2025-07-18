// Data structures

// Tuples (common with Spark)
// Immutable lists

val captainStuff = ("Picard", "Enterprise-D", "NCC-1701-D")
println(captainStuff)

// You refer to individual fields with their ONE-BASED index:
println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

// You can create a key/value pair with ->
val picardsShip = "Picard" -> "Enterprise-D"
println(picardsShip._2)

// You can mix different types in a tuple
val aBunchOfStuff = ("Kirk", 1964, true)

// Lists
// Like a tuple, but it's an actual Collection object that has more functionality.
// Also, it cannot hold items of different types.
// It's a singly-linked list under the hood.

val shipList = List("Enterprise", "Defiant", "Voyager", "Deep Space Nine")

// Access individual members using () with ZERO-BASED index (confused yet?)
println(shipList(1))

// head and tail give you the first item, and the remaining ones.
println(shipList.head)
println(shipList.tail)


// Iterating though a list
for (ship <- shipList) {println(ship)}

// Let's apply a function literal to a list! map() can be used to apply any function to every item in a collection.
val backwardShips = shipList.map( (ship: String) => {ship.reverse})
for (ship <- backwardShips) {println(ship)}

// reduce() can be used to combine together all the items in a collection using some function.
val numberList = List(1, 2, 3, 4, 5)
val sum = numberList.reduce( (x: Int, y: Int) => x + y)
println(sum)

// filter() can remove stuff you don't want. Here we'll introduce wildcard syntax while we're at it.
val iHateFives = numberList.filter( (x: Int) => x != 5)
val iHateThrees = numberList.filter(_ != 3)

// Note that Spark has its own map, reduce, and filter functions that can distribute these operations. But they work the same way!
// Also, you understand MapReduce now :)

// Concatenating lists
val moreNumbers = List(6, 7, 8)
val lotsOfNumbers = numberList ++ moreNumbers

// More list fun
val reversed = numberList.reverse
val sorted = reversed.sorted
val lotsOfDuplicates = numberList ++ numberList
val distinctValues = lotsOfDuplicates.distinct
val maxValue = numberList.max
val total = numberList.sum
val hasThree = iHateThrees.contains(3)

// Maps
// Useful for key/value lookups on distinct keys
// Like dictionaries in other languages

val shipMap = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D", "Sisko" -> "Deep Space Nine", "Janeway" -> "Voyager")
println(shipMap("Janeway"))

// Dealing with missing keys
println(shipMap.contains("Archer"))

val archersShip = util.Try(shipMap("Archer")) getOrElse "Unknown"
println(archersShip)

// EXERCISE
// Create a list of the numbers 1-20; your job is to print out numbers that are evenly divisible by three. (Scala's
// modula operator, like other languages, is %, which gives you the remainder after division. For example, 9 % 3 = 0
// because 9 is evenly divisible by 3.) Do this first by iterating through all the items in the list and testing each
// one as you go. Then, do it again by using a filter function on the list instead.

val numbersList = List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)

for (currentNumber <- numbersList) {if(currentNumber % 3 == 0){println(currentNumber)}}

val filteredList = numbersList.filter(_ %3 == 0)
println(filteredList)