package par1recap

/**
  * @author sahilgogna on 2020-05-03
  */
object ScalaRecap extends App {

  // instruction vs expression
  val theUnit = println("No meaningful output")

  // functions
  def myFunction(x:Int):Int = 42

  // classes
  class Animal
  class Dog extends Animal
  trait Carnivore{
    def eat(animal:Animal) : Unit
  }

  class Crocodile extends Animal with Carnivore{
    override def eat(animal: Animal): Unit = println("nom nom")
  }

  // objects are singleton
  object MySingleton

  // in scala if you have a class or trait with a same name as a singleton object, it is called a companion
  // companions
  object Carnivore

  // try catch, future , partial functions

  // implicits -> auto injection by the compiler


}
