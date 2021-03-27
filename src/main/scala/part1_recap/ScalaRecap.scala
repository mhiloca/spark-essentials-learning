package part1_recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  // expressions are evaluated and need to reduced to a single value

  val theUnit = println("Hello, Scala!") // Unit = "no meaningful value" -> void in other languages

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal

  class Cat extends Animal

  trait Carnivore {
    // with traits we can implement abstract unimplemented methods
    def eat(anima: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(anima: Animal): Unit =
      println("Crunch")
  }

  // singleton pattern
  object MySingleton

  // companions
  object Carnivore

  // generics
  trait MyList[+A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming - functions in scala are extentions of Fucntion1, Function2... Function22
  val incrementer: Int => Int = x => x + 1 // lambda (you are instantiating one of the traits above)
  val incremented = incrementer(41) // 42

  // map, flatMap, filter - HOFs
  val processedList = List(1, 2, 3).map(incrementer)

  // Pattern Matching
  val unkownn: Any = 45
  val ordinal = unkownn match { // it's inferred to be a String because all the cases return String
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try+catch
  try {
    throw new NullPointerException
  } catch { // this is constructed via pattern matching
    case e: NullPointerException => "return some value"
    case _ => "something else"
  }

  // Futures

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computations, runs on anther thread
    42 // futures are typed with the type with the value the contain after they are completed on whatever thread they n
  }

  aFuture.onComplete {
    case Success(value) => println(s"I've found the meaning of life $value")
    case Failure(e) => println(s"I've failed")
  }

  // Partial Functions
  val aPartialFunction: Int => Int = {
    case 1 => 42
    case 2 => 65
    case _ => 999
  }

  // Implicits
  // auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgument

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet

  // implicit conversions - implicit classes - almost always preferable to implicit defs
  implicit class Dog(name: String) {
    def bark = println("Bark")
  }

  "Lassie".bark

  /*
    - local scope
    - import scope
    - companion objects of the type involved in the method call
   */

  List(1, 2, 3).sorted


}
