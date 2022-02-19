import org.apache.spark.graphx.{EdgeDirection, Graph, VertexRDD}

object Bubbles {

  def SNP (g: Graph[Kmer, Integer], k: Integer) : VertexRDD[(Kmer, List[Long], String, Int, Integer)] = {
    // GRAFO INIZIALE
    // attr._1 : kmer
    // attr._2 : List(-1) mi serve per contenere il long da passare
    // attr._3 : "None" stringa che userò per la sequenza
    // attr._4 : 0 iterazione
    // attr._5 : outdegree
    var graph = g.outerJoinVertices(g.outDegrees)((_, attr, degOpt) => (attr, List(-1L), "None", 0, degOpt.getOrElse(0).asInstanceOf[Integer]))
    graph = graph.outerJoinVertices(graph.degrees)((vid, attr, degOpt) => (attr._1, if (attr._5 >= 2) List(vid) else attr._2, if (attr._5 >= 2) attr._1.toString else attr._3, 0, degOpt.getOrElse(0).asInstanceOf[Integer]))
    //graph = graph.mapVertices((id,attr) => (attr._1, if (attr._2 >= 2) List(id.toInt) else attr._3, if (attr._2 >= 2) attr._1.toStringOriginal else attr._4, 0, attr._6))
    // GRAFO FINALE
    // attr._1 : kmer
    // attr._2 : List(Long) dentro c'è -1 se outdegree < 2 altrimenti prende il long
    // attr._3 : String dentro c'è la stringa del kmer in forma originale se outdegree >= 2 altrimenti "None"
    // attr._4 : Int le iterazioni
    // attr._5 : degree

    // Come messaggio iniziale abbiamo (List(-1), "None", 0) ossia caso in cui outdegree < 2 e l'iterazione è 0
    graph = graph.pregel(Tuple3(List(-1L), "None", 0), activeDirection=EdgeDirection.Out, maxIterations = k+1)(
      // Se siamo all'iterazione 0 e il vertice ha outdegree >= 2 (ossia trovo in List qualcosa di diverso da -1 e in String
      // qualcosa di diverso da "None") allora utilizzo gli attributi del vertice, altrimenti gli passo quelli del messaggio
      // in ogni caso aggiorno il conteggio delle iterazioni
      (_, vertex, new_visited) => (vertex._1, if(!vertex._2.head.equals(-1) && new_visited._3==0) vertex._2 else new_visited._1, if(!vertex._3.equals("None") && new_visited._3==0) vertex._3 else new_visited._2, new_visited._3, vertex._5), //vprog

      triplet => { //sendMsg
        // CASO iterazioni != k
        // Il messaggio passa solo se il vertice destinazione ha degree == 2 (triplet.dstAttr._5==2) e in List c'è -1 (triplet.dstAttr._2.head.equals(-1)) (questo vuol dire che il suo outdegree < 2)
        // allo stesso tempo il vertice sorgente deve avere List con un long (!triplet.srcAttr._2.head.equals(-1))) quindi o gli è arrivato un messaggio o è il vertice di partenza con outdegree >= 2
        if ((triplet.dstAttr._2.head.equals(-1L) && !triplet.srcAttr._2.head.equals(-1L)) && triplet.srcAttr._4!=k && triplet.dstAttr._5==2) {
          // In questo caso il messaggio sarà composto dal List del vertice sorgente, la Stringa del vertice sorgente e l'iterazione aggiornata
          Iterator((triplet.dstId, Tuple3(triplet.srcAttr._2, triplet.srcAttr._3, triplet.srcAttr._4+1)))
        }
        // CASO iterazioni == k
        // Mi serve per essere certa che il messaggio arrivi al vertice di chiusura anche se questo ha outdegree >= 2
        else if (triplet.srcAttr._4==k) {
          // In questo caso il messaggio sarà composto dal List del vertice sorgente, dalla Stringa concatenata tra la Stringa del vertice
          // sorgente, la prima lettera della Stringa del kmer sorgente e la stringa del vertice destinazione
          // Praticamente ad ogni iterazione mi passo la stringa del vertice di apertura della bolla, che è sempre la stessa, quando
          // arrivo nel vertice di chiusura ho già il suffisso che mi serve per la sequenza mi manca solo la mutazione centrale e la prendo
          // dalla prima lettere della sequenza del vertice sorgente
          Iterator((triplet.dstId, Tuple3(triplet.srcAttr._2, triplet.srcAttr._3.concat(triplet.srcAttr._1.toString.substring(0,1)).concat(triplet.dstAttr._1.toString), triplet.srcAttr._4+1)))
        } else
          Iterator.empty
      },
      // In caso di più messaggi che arrivano allo stesso vertice
      // NOTA: succede solo nel vertice di chiusura perchè in tutti gli altri casi questo non è reso possibile dalle costrizioni
      // su outdegree e degree
      (visited1, visited2) => Tuple3(List.concat(visited1._1, visited2._1), visited1._2 + " " + visited2._2, visited2._3)//mergeMsg
    )
    //graph.vertices.collect().foreach(v=>println(v))
    //System.out.println("Stop")
    val vertices = graph.vertices.filter{
      case (_, attr) => (attr._4 == k+1) && (attr._2.length>=2)
      case _ => false
    }
    vertices.foreach(v => println(v))
    vertices
  }

}
