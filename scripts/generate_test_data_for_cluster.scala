import util.Random
import java.io._

def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
try { f(param) } finally { param.close() }

def appendToFile(fileName:String, textData:String) =
  using (new FileWriter(fileName, true)){ 
    fileWriter => using (new PrintWriter(fileWriter)) {
      printWriter => printWriter.println(textData)
    }
  }
  
1 to 1000 foreach { _ => appendToFile("testVectors","{"+Random.nextInt(4)+","+Random.nextInt(4)+","+Random.nextInt(4)+","+Random.nextInt(4)+"},") }

