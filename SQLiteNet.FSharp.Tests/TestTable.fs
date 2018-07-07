module SQLiteNet.FSharp.Tests.TestTable

open SQLite

open NUnit.Framework
open FsUnitTyped

type IDTOConverter<'a, 'dto> =
    abstract member Convert     : 'a   -> 'dto
    abstract member ConvertBack : 'dto -> 'a

type Table<'dto when 'dto : (new : unit -> 'dto)>(db : SQLiteConnection) =

    let t =
        db.CreateTable<'dto>() |> ignore
        db.Table<'dto>()

    let selectByPrimaryKey =
        sprintf
            "SELECT * FROM %s WHERE %s = ?"
            t.Table.TableName
            t.Table.PK.Name

    member __.Insert (x : 'dto) = db.Insert(x) |> ignore
    
    member __.Read () = t :> seq<'dto>

    member __.Read key : 'dto =
        db.Query<'dto>(selectByPrimaryKey, [| key |])
        |> Seq.exactlyOne

type Table<'a, 'dto when 'dto : (new : unit -> 'dto)>(db : SQLiteConnection, c : IDTOConverter<'a, 'dto>) =

    let t =
        db.CreateTable<'dto>() |> ignore
        db.Table<'dto>()

    let selectByPrimaryKey =
        sprintf
            "SELECT * FROM %s WHERE %s = ?"
            t.Table.TableName
            t.Table.PK.Name

    member __.Insert (x : 'a) = db.Insert(c.Convert x) |> ignore
    
    member __.Read () = t |> Seq.map c.ConvertBack

    member __.Read key : 'a =
        db.Query<'dto>(selectByPrimaryKey, [| key |])
        |> Seq.exactlyOne
        |> c.ConvertBack

[<CLIMutable>]
type SimpleRecord =
    {
        [<PrimaryKey>]
        Key     : string

        Integer : int
    }

type DU =
    | DUString  of string
    | DUInteger of int

type DURecord =
    {
        Key : string
        DU  : DU
    }

type ComplexRecord =
    {
        Key : string

        SimpleRecord : SimpleRecord
        DURecord     : DURecord
    }

[<Test>]
let ``Insertion and reading of simple records works correctly`` () =
    use db = new SQLiteConnection(":memory:") 
    let table = Table<SimpleRecord>(db)

    table.Insert { Key = "A"; Integer = 1 }
    table.Insert { Key = "B"; Integer = 2 }
    table.Insert { Key = "C"; Integer = 3 }

    table.Read()
    |> Array.ofSeq
    |> shouldEqual
        [|
            { Key = "A"; Integer = 1 }
            { Key = "B"; Integer = 2 }
            { Key = "C"; Integer = 3 }
        |]

module Storage =
    [<CLIMutable>]
    type DURecordDTO =
        {
            [<PrimaryKey>]
            Key : string

            DUTag     : int
            DUString  : string
            DUInteger : int
        }

    [<CLIMutable>]
    type ComplexRecordDTO =
        {
            [<PrimaryKey>]
            Key : string

            SimpleRecordKey : string
            DURecordKey     : string
        }

    type Converters =
        static member DURecord =
            {
                new IDTOConverter<DURecord,_> with
                    member __.Convert x =
                        let dutag, dus, dui =
                            match x.DU with
                            | DUString  s -> 0,  s, 0
                            | DUInteger n -> 1, "", n

                        {
                            Key = x.Key

                            DUTag     = dutag
                            DUString  = dus
                            DUInteger = dui
                        }

                    member __.ConvertBack x =
                        {
                            Key = x.Key

                            DU =
                                match x.DUTag with
                                | 0 -> DUString  x.DUString
                                | 1 -> DUInteger x.DUInteger
                                | n -> failwithf "Unknown value: %d" n
                        }
            }

        static member ComplexRecord (simpleRecordTable : Table<SimpleRecord>) (duRecordTable : Table<DURecord, _>) =

            {
                new IDTOConverter<ComplexRecord,_> with
                    member __.Convert x =
                        simpleRecordTable.Insert x.SimpleRecord
                        duRecordTable.Insert x.DURecord

                        {
                            Key = x.Key

                            SimpleRecordKey = x.SimpleRecord.Key
                            DURecordKey = x.DURecord.Key
                        }

                    member __.ConvertBack x =
                        {
                            Key = x.Key

                            SimpleRecord = simpleRecordTable.Read x.SimpleRecordKey
                            DURecord     = duRecordTable.Read     x.DURecordKey
                        }
            }

[<Test>]
let ``Insertion and retrieval of records containing discriminated unions works correctly`` () =
    use db = new SQLiteConnection(":memory:") 
    let table = Table<DURecord, Storage.DURecordDTO>(db, Storage.Converters.DURecord)

    table.Insert { Key = "A"; DU = DUInteger 1     }
    table.Insert { Key = "B"; DU = DUInteger 2     }
    table.Insert { Key = "C"; DU = DUString  "DEF" }

    table.Read ()
    |> Array.ofSeq
    |> shouldEqual
        [|
            { Key = "A"; DU = DUInteger 1     }
            { Key = "B"; DU = DUInteger 2     }
            { Key = "C"; DU = DUString  "DEF" }
        |]

[<Test>]
let ``Insertion and retrieval of complex records works correctly`` () =

    use db = new SQLiteConnection(":memory:") 

    let table =
        Table<ComplexRecord, Storage.ComplexRecordDTO>
            (
                db,
                Storage.Converters.ComplexRecord
                    (Table<SimpleRecord>(db))
                    (Table<DURecord, Storage.DURecordDTO>(db, Storage.Converters.DURecord))
            )

    let data =
        [|
            { Key = "C01"; SimpleRecord = { Key = "A"; Integer = 1 }; DURecord = { Key = "A"; DU = DUInteger 1     } }
            { Key = "C02"; SimpleRecord = { Key = "B"; Integer = 2 }; DURecord = { Key = "B"; DU = DUInteger 2     } }
            { Key = "C03"; SimpleRecord = { Key = "C"; Integer = 3 }; DURecord = { Key = "C"; DU = DUString  "DEF" } }
        |]
    
    data |> Array.iter table.Insert

    table.Read ()
    |> Array.ofSeq
    |> shouldEqual data
