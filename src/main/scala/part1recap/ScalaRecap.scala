package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expression
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instruction(C, C++ Programming) vs expression(Scala Programming)
  val theUnit = println("Hello, Scala")
  // Unit = "no meaningful value" or void in other languages
  // printing something in console it'll return unit

  // functions
  def myFunction(x: Int) = 42


  // OOP
  class Animal

  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern // we can define two object in the same scala file
  object MySingleton {
    // methods, members(val, var)
  }

  // companions --> in scala we have same name of class and object it's called companions object
  object Carnivore // trait Carnivore and object Carnivore are called Companions

  // generics
  trait MyList[A]

  // method notation (infix, postfix, prefix) mostly we used in infix
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementer: Function1[Int, Int] = new Function1[Int, Int] {
    override def apply(x: Int): Int = x + 1
  }

  val incrementor1: Int => Int = x => x + 1 // we can use this also it's called LAMBDA
  val incremented = incrementer(42)

  // map, flatMap, filter  = all are HOF means they received functions as arguments
  val processedList = List(1, 2, 3).map(incrementer)

  // pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match { // it will return 'String' type because all case return 'String'
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException()
  } catch {
    case e: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // Future

  import scala.concurrent.ExecutionContext.Implicits.global // imported scope

  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s" I've found $meaningOfLife")
    case Failure(ex) => println(s"I have failed : $ex")
  }

  // Partial Functions
  // val aPartialFunction = (x : Int) => x match {
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits

  // auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val implicitInt = 67 // local scope

  // methodWithImplicitArgument(67) // pass argument explicitly otherwise without argument we can just call that method

  val implicitCall = methodWithImplicitArgument
  // compiler automatically inject value of 67 + 43

  // Implicit Conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet // compiler automatically call fromStringToPerson("Bob").greet

  // implicit conversion - implicit classes
  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }

  "Lassie".bark

  /*
    - local scope --> implicit val implicitInt = 67
    - imported scope --> import scala.concurrent.ExecutionContext.Implicits.global // global is implicit
    - companion objects of the types involved in the method call  -->
      - Ex: List(1,2,3).sorted // sorted[B >: A](implicit ord: Ordering[B]) compiler looking for implicit ordering for [Int]

  */



}




















