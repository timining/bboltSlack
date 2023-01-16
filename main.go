package main

//Explicaciones y asunciones

//Diagrama de celdas
// ____________________________________
//|C91|C92|C93|C94|C95|C96|C97|C98|C99|C100|
//|C81|C82|C83|C84|C85|C86|C87|C88|C89|C90|
//|C71|C72|C73|C74|C75|C76|C77|C78|C79|C80|
//|C61|C62|C63|C64|C65|C66|C67|C68|C69|C70|
//|C51|C52|C53|C54|C55|C56|C57|C58|C59|C60|
//|C41|C42|C43|C44|C45|C46|C47|C48|C49|C50|
//|C31|C32|C33|C34|C35|C36|C37|C38|C39|C40|
//|C21|C22|C23|C24|C25|C26|C27|C28|C29|C30|
//|C11|C12|C13|C14|C15|C16|C17|C18|C19|C20|
//| C1| C2| C3| C4| C5| C6| C7| C8| C9|C10|
//
//Matriz de 10x10 = 100 celdas
//Ancho = eje X (i)
//Alto = eje Y (j)

// Si anchoTile = 2 y altoTile = 2
// NTiles= (10/2)*(10/2)=5*5= 25 tiles
// ____________________________________
//|T21|T21|T22|T22|T23|T23|T24|T24|T25|T25|
//|T21|T21|T22|T22|T23|T23|T24|T24|T25|T25|
//|T16|T16|T17|T17|T18|T18|T19|T19|T20|T20|
//|T16|T16|T17|T17|T18|T18|T19|T19|T20|T20|
//|T11|T11|T12|T12|T13|T13|T14|T14|T15|T15|
//|T11|T11|T12|T12|T13|T13|T14|T14|T15|T15|
//| T6| T6| T7| T7| T8| T8| T9| T9|T10|T10|
//| T6| T6| T7| T7| T8| T8| T9| T9|T10|T10|
//| T1| T1| T2| T2| T3| T3| T4| T4| T5| T5|
//| T1| T1| T2| T2| T3| T3| T4| T4| T5| T5|
//

//NTilesX = NceldasX/anchoTile = 5
// tileCelda(i,j) = ((j/altoTile)*NTilesX)-(NTilesX-techo(i/anchoTile))

//Ej: celda(7,6) = (techo(6/2)*5)-(5-techo(7/2)) = (3*5)-(5-4) = 15 - 1 = 14
//Ej: celda(2,4) = (techo(4/2)*5)-(5-techo(2/2)) = (2*5)-(5-1) = 10 - 4 = 6
//Ej: celda(4,8) = (techo(8/2)*5)-(5-techo(4/2)) = (4*5)-(5-2) = 20 - 3 = 17
//Ej: celda(9,3) = (techo(3/2)*5)-(5-techo(9/2)) = (2*5)-(5-5) = 10 - 0 = 10
//Ej: celda(5,7) = (techo(7/2)*5)-(5-techo(5/2)) = (4*5)-(5-3) = 20 - 2 = 18
import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type cell struct {
	height int
	date   time.Time
}

const anchoTile = 100.0
const altoTile = 100.0

var alto int
var ancho int

//1 tile = 100x100 celdas
// cuantos tiles tiene mi topo??? tl=largo/100 y ta=ancho/100 ===> int(ta)*int(tl)
// nombres de los buckets == tile_n
var wg sync.WaitGroup
var bucketList []string

func main() {

	flag.IntVar(&ancho, "ancho", ancho, "ancho de la topo")
	flag.IntVar(&alto, "largo", alto, "largo de la topo")

	start := time.Now()
	fmt.Println("im testing Bbolt")
	db, err := bolt.Open("myTestDatabase.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		fmt.Print(err.Error())
		return
	}

	flag.Parse()

	if flag.NFlag() != 2 {
		fmt.Printf("Flags: %d\n\r", flag.NArg())
		flag.PrintDefaults()
		return
	}

	// calcular cuantos tiles

	ta := ancho / anchoTile //EjeX
	tl := alto / altoTile   //EjeY

	tiles := int(tl) * int(ta)

	// fmt.Printf("Largo: %d\n\r", alto)
	// fmt.Printf("ancho: %d\n\r", ancho)

	// Un bucket por cada tile
	for i := 1; i <= tiles; i++ {
		name := fmt.Sprintf("tile_%d", i)
		CreateBucket(db, name)
		fmt.Println("bucket: ", i)
		bucketList = append(bucketList, name)
	}

	// CreateBucket(db, "testBucket")

	// wg.Add(alto * ancho)

	// for j := 1; j <= alto; j++ {
	// 	for i := 1; i <= ancho; i++ {
	// 		tile := getCellTile(i, j)
	// 		c := cell{
	// 			height: rand.Intn(1000),
	// 			date:   time.Now(),
	// 		}
	// 		// fmt.Println("inserting cell on Tile: ", tile)
	// 		go InsertCell(db, tile, &c)
	// 	}
	// }

	// for i := 0; i < (largo * ancho); i++ {
	// 	tile := getCellTile()
	// 	c := cell{
	// 		height: rand.Intn(1000),
	// 		date:   time.Now(),
	// 	}
	// 	go InsertCell(db, tile, &c)
	// }

	// var cells []*cell
	// for i := 0; i < (largo * ancho); i++ {
	// 	c := cell{
	// 		height: rand.Intn(1000),
	// 		date:   time.Now(),
	// 	}
	// 	cells = append(cells, &c)
	// }

	// go InsertCellsBatch(db, cells)

	defer db.Close()
	// wg.Wait()

	for _, b := range bucketList {
		go GetCellRangeInBucket(db, b, time.Now().Add(-10*time.Minute), time.Now())
	}
	// go GetCellBatch(db)
	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("BboltSlack demoro %s", elapsed)
	fmt.Println("FINISH")
}

func getCellTile(i, j int) string {

	// tileCelda(i,j) = ((j/altoTile)*NTilesX)-(NTilesX-techo(i/anchoTile))
	ntilesX := ancho / anchoTile
	tileY := math.Ceil(float64(j) / float64(altoTile))
	tileX := math.Ceil(float64(i) / float64(anchoTile))

	t := (int(tileY) * ntilesX) - (ntilesX - int(tileX))
	name := fmt.Sprintf("tile_%d", t)
	return name
}

func CreateBucket(db *bolt.DB, name string) {
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(name))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
}

func InsertCell(db *bolt.DB, bucket string, c *cell) {
	db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			fmt.Println("BUCKET NOT EXIST: ", bucket)
			return nil
		}
		err := b.Put([]byte(c.date.String()), []byte(strconv.Itoa(c.height)))
		if err != nil {
			fmt.Println("ERROR ON CELL INSERTION:", err)
			return err
		}
		// }
		return nil
	})
	defer wg.Done()
}

// func InsertCellsBatch(db *bolt.DB, cells []*cell) {

// 	wg.Add(1)
// 	fmt.Printf("cantidad de celdas: %d\n\r", len(cells))
// 	err := db.Batch(func(tx *bolt.Tx) error {
// 		for i, c := range cells {
// 			bucket := getCellTileByIndex(i) //Not Implemented Yet
// 			b := tx.Bucket([]byte(bucket))
// 			insertionDate := time.Now()
// 			data, err := json.Marshal(c)
// 			if err != nil {
// 				return err
// 			}

// 			err = b.Put([]byte(insertionDate.String()), data)
// 			if err != nil {
// 				return err
// 			}
// 			// }
// 			return nil
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return
// 	}
// 	defer wg.Done()
// }

func GetSingleCell(db *bolt.DB, bucket string, key time.Time) {
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		cell := b.Get([]byte(key.String()))
		fmt.Println(cell)
		return nil
	})
}

func GetCellRangeInBucket(db *bolt.DB, bucket string, ini time.Time, end time.Time) {
	wg.Add(1)
	db.View(func(tx *bolt.Tx) error {

		fmt.Println("Searching for cells range")
		// Assume our events bucket exists and has RFC3339 encoded time keys.
		c := tx.Bucket([]byte(bucket)).Cursor()

		min := []byte(ini.String())
		max := []byte(end.String())

		// Iterate over the min and max.
		for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
			fmt.Printf("%s: %s\n", k, v)
		}

		return nil

	})
	defer wg.Done()
}

func GetAllCellsInBucket(db *bolt.DB, bucket string) {
	wg.Add(1)
	fmt.Println("Getting al values on a bucket")
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}

		return nil
	})
	defer wg.Done()
}
