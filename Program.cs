using System.ComponentModel.Design;
using System.Diagnostics;
using System.Globalization;
using System.Net.Quic;
using SaintCoinach.Xiv;

const string GameDirectory = @"D:\SteamLibrary\steamapps\common\FINAL FANTASY XIV Online";
var realm = new SaintCoinach.ARealmReversed(GameDirectory, SaintCoinach.Ex.Language.English);
var connString = "Host=localhost:32768;Username=postgres;Password=doritofam;Database=postgres";
await using var dataSource = Npgsql.NpgsqlDataSource.Create(connString);

var items = realm.GameData.GetSheet<SaintCoinach.Xiv.Item>();
var recipes = realm.GameData.GetSheet<SaintCoinach.Xiv.Recipe>();
var worlds = realm.GameData.GetSheet("World");


async Task dumpAllItems(IEnumerable<SaintCoinach.Xiv.Item> items) {
    CoinachSQLDump.SQLTableCreation.ItemEntry itemToLoad;
    List<Task> sqlLoadTasks = [];
    foreach(var i in items) {
        itemToLoad = new CoinachSQLDump.SQLTableCreation.ItemEntry {
            ItemID = i.Key,
            Type = i.ItemUICategory.Name,
            Name = i.Name,
            ItemLevel = i.ItemLevel.Key,
            EquipmentTypes = i.EquipSlotCategory.PossibleSlots.Select(s => s.Name),
            HighQualityable = i.CanBeHq,
            Marketable = !i.IsUntradable,
            GilPrice = i.Ask > 0 ? i.Ask : 0,
            ClassJobRestriction = i.ItemSearchCategory.ClassJob.Name,
        };
        List<string> origins = [];
        foreach (var source in i.Sources) {
            switch(source) 
            {
                case SaintCoinach.Xiv.Recipe r:
                    origins.Add("Crafted");
                    break;
                case SaintCoinach.Xiv.RetainerTaskBase:
                    origins.Add("Retainer");
                    break;
                case SaintCoinach.Xiv.Quest q:
                    origins.Add("Quest");
                    break;
                case SaintCoinach.Xiv.Leve l:
                    origins.Add("Leve");
                    break;
                case SaintCoinach.Xiv.GilShop gs:
                    origins.Add("Gil Merchant");
                    break;
                case SaintCoinach.Xiv.GatheringPointBase gpb:
                    origins.Add("Gathering");
                    break;
                case SaintCoinach.Xiv.GCShop gcs:
                    origins.Add("GC Shop");
                    break;
                case SaintCoinach.Xiv.FishingSpot fs:
                    origins.Add("Fishing");
                    break;
                case SaintCoinach.Xiv.FccShop fccs:
                    origins.Add("FC Shop");
                    break;
                case SaintCoinach.Xiv.CompanyCraftSequence ccs:
                    origins.Add("FC Craft");
                    break;
                case SaintCoinach.Xiv.Achievement ach:
                    origins.Add("Achievement");
                    break;
                case SaintCoinach.Xiv.SpecialShop specShop:
                    origins.Add("Special Shop");
                    bool found = false;
                    foreach(var listing in specShop.Items) {
                        if (found) {
                            break;
                        }
                        foreach(var soldItem in listing.Rewards) {
                            if (soldItem.Item.Key == i.Key) {
                                found = true;
                                if (listing.Costs.Count() > 0) {
                                    itemToLoad.SpecialCurrencyItemID = listing.Costs.First().Item.Key;
                                    itemToLoad.SpecialCurrencyCount = listing.Costs.First().Count;
                                }
                                break;
                            }
                        }
                    }
                    break;
                default:
                    break;

            }
        }
        sqlLoadTasks.Add(CoinachSQLDump.SQLTableCreation.LoadItem(dataSource, itemToLoad));
    }
    await Task.WhenAll(sqlLoadTasks);
    return;
}

async Task dumpAllRecipes(IEnumerable<SaintCoinach.Xiv.Recipe> recipes) {
    List<Task> loadTasks = [];
    CoinachSQLDump.SQLTableCreation.RecipeEntry recipeToLoad;
    foreach(var r in recipes) {
        recipeToLoad = new CoinachSQLDump.SQLTableCreation.RecipeEntry {
            RecipeID = r.Key,
            CraftedItemID = r.ResultItem.Key,
            CraftedItemCount = r.ResultCount,
            Ingredients = r.Ingredients.Select(i => new CoinachSQLDump.SQLTableCreation.Ingredient{
                IngredientID = i.Item.Key,
                Quantity = i.Count,
            }),
        };
        loadTasks.Add(CoinachSQLDump.SQLTableCreation.LoadRecipe(dataSource, recipeToLoad));
    }
    await Task.WhenAll(loadTasks);
    return;
}

async Task dumpAllWorlds(IEnumerable<XivRow> worlds) {
    List<Task> loadTasks = [];
    CoinachSQLDump.SQLTableCreation.WorldEntry worldToLoad;

    foreach(var world in worlds) {
        string? name = (string?) world["Name"].ToString();
        string? datacenter = (string?) world["DataCenter"].ToString();
        bool isPublic = world["IsPublic"].ToString() == "True" ? true : false;

        if (name == null || datacenter == null) {
            Console.WriteLine($"Unexpected failed lookup for name or datacenter on World sheet row {world.Key}");
            return;
        }

        worldToLoad = new CoinachSQLDump.SQLTableCreation.WorldEntry {
            WorldID = world.Key,
            Name = name,
            Datacenter = datacenter,
            IsPublic = isPublic,
        };

        loadTasks.Add(CoinachSQLDump.SQLTableCreation.InsertWorldEntry(dataSource, worldToLoad));
    }

    await Task.WhenAll(loadTasks);
    return;
}

bool quit = false;
while (true){
    if (quit) {
        break;
    }
    Console.WriteLine("create: Create all SQL tables");
    Console.WriteLine("delete: Delete all SQL tables");
    Console.WriteLine("load items: Load all items into SQL");
    Console.WriteLine("load recipes: Load All Recipes");
    Console.WriteLine("create worlds: create worlds table");
    Console.WriteLine("load worlds: dump worlds into SQL");
    Console.WriteLine("quit");
    var input = Console.ReadLine();
    switch(input) {
        case "create":
            await CoinachSQLDump.SQLTableCreation.CreateAllTables(dataSource);
            break;
        case "delete":
            await CoinachSQLDump.SQLTableCreation.DeleteAllTables(dataSource);
            break;
        case "load items":
            await dumpAllItems(items);
            break;
        case "load recipes":
            await dumpAllRecipes(recipes);
            break;
        case "create worlds":
            await CoinachSQLDump.SQLTableCreation.CreateWorldsTable(dataSource);
            break;
        case "load worlds":
            await dumpAllWorlds(worlds);
            break;
        case "quit":
            quit = true;
            break;
        default:
            Console.WriteLine("invalid input");
            break;
    }

}

dataSource.Dispose();

//foreach(var cfc in contentFinder) {
//    if (cfc.Key == 759) {
//        Console.WriteLine(cfc.Name);
//        foreach(var instance in encounters) {
//            if (instance.Key == cfc.Content.Key) {
//                var source = (IItemSource) instance;
//                foreach(var item in source.Items) {
//                    Console.WriteLine(item.Name);
//                }
//            }
//        }
//    }
//}