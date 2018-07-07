module SQLiteNet.FSharp.Tests.TestBasicUse

open SQLite

open NUnit.Framework
open FsUnitTyped

[<CLIMutable>]
type SimpleRecord =
    {
        [<PrimaryKey>]
        String : string
        Integer : int
    }

[<Test>]
let ``Insertion and reading of simple records works correctly`` () =
    use db = new SQLiteConnection(":memory:") 
    db.CreateTable<SimpleRecord>() |> ignore

    db.Insert { String = "A"; Integer = 1 } |> ignore
    db.Insert { String = "B"; Integer = 2 } |> ignore
    db.Insert { String = "C"; Integer = 3 } |> ignore

    let t = db.Table<SimpleRecord>()
    t
    |> Array.ofSeq
    |> shouldEqual
        [|
            { String = "A"; Integer = 1 }
            { String = "B"; Integer = 2 }
            { String = "C"; Integer = 3 }
        |]

type DU =
    | DUString  of string
    | DUInteger of int

type DURecord =
    {
        Key : string
        DU  : DU
    }

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

    let convert (x : DURecord) =
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

    let convertBack x =
        {
            Key = x.Key

            DU =
                match x.DUTag with
                | 0 -> DUString  x.DUString
                | 1 -> DUInteger x.DUInteger
                | n -> failwithf "Unknown value: %d" n
        }

[<Test>]
let ``Insertion and retrieval of records containing discriminated unions works correctly`` () =
    use db = new SQLiteConnection(":memory:") 
    db.CreateTable<Storage.DURecordDTO>() |> ignore

    db.Insert (Storage.convert { Key = "A"; DU = DUInteger 1     }) |> ignore
    db.Insert (Storage.convert { Key = "B"; DU = DUInteger 2     }) |> ignore
    db.Insert (Storage.convert { Key = "C"; DU = DUString  "DEF" }) |> ignore

    let t = db.Table<Storage.DURecordDTO>()
    t
    |> Seq.map Storage.convertBack
    |> Array.ofSeq
    |> shouldEqual
        [|
            { Key = "A"; DU = DUInteger 1     }
            { Key = "B"; DU = DUInteger 2     }
            { Key = "C"; DU = DUString  "DEF" }
        |]
