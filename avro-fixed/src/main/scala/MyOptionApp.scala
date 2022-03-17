object MyOptionApp extends App {
  def skipNot(state: String, newId: Option[Int] = None, oldId: Option[Int] = None, newVer: Option[String] = None, oldVer: Option[String] = None) = {
    val schemaDirection = Ordering[Option[Int]].compare(newId, oldId)
    val versionDirection = Ordering[Option[String]].compare(newVer, oldVer)

    val res1 = state == "CREATE" && (schemaDirection >= 0 || versionDirection >= 0)
    val res2 = state == "FAILED" && (schemaDirection >  0 || versionDirection >  0)

    val res = (res1 || res2)
    println(s"$state = (newId: $newId   oldId: $oldId  newVer: $newVer oldVer: $oldVer schemaDirection: $schemaDirection  versionDirection: $versionDirection  res1: $res1  res2: $res2) : $res")

    res
  }

  skipNot("CREATE") //true
  skipNot("FAILED", Some(1), None, Some("15.3.5")) //true
  skipNot("FAILED", Some(1), Some(1), Some("15.3.5"), Some("15.3.5")) //no change - false
  skipNot("FAILED", Some(1), Some(1), Some("15.3.7"), Some("15.3.5")) //new app ver - true
  skipNot("FAILED", Some(2), Some(1), Some("15.3.5"), Some("15.3.5")) //new schema id - true
  skipNot("CREATE", Some(2), None, Some("15.4.5")) //new app ver - true
}
