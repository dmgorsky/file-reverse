package example

import org.scalatest._

class InterviewSpec extends FlatSpec with Matchers {
  "The Interview object" should "say hello" in {
    Interview.greeting shouldEqual "hello"
  }
}
