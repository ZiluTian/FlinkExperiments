package simulations

object measure {
    def apply(program: ()=> Unit, iterations: Int): Unit = {
        val begin = System.currentTimeMillis() 
        program()
        val end = System.currentTimeMillis() 
        println(f"Average time per iteration is ${(end - begin) / iterations} ms")
    }
}