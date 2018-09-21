import scala.math._

class Shape3D(val height: Double) {
  var h: Double = height
  var result: Double = _
  def volume() {
    result = h * h * h
    println("Volume of shape (cube): " + result)
  }
}

class Cylinder (override val height: Double, val radius: Double) extends Shape3D(height) {
  var r: Double = radius

  override def volume() {
    result = Pi * r * r * h
    println("Volume of cylinder: " + result)
  }
}

object Shape3D {
  def main(args: Array[String]): Unit = {
    val shape = new Cylinder(2,1)
    shape.volume()
  }
}
