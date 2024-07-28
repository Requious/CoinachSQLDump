using System.Reflection.Metadata.Ecma335;
using System.Runtime.CompilerServices;
using Npgsql;
using SaintCoinach.Libra;
using SaintCoinach.Xiv;

namespace CoinachSQLDump
{
    public class SQLTableCreation
    {
        public struct ItemEntry {
            public int ItemID { get; set; }
            public string Type { get; set; }
            public IEnumerable<string> Origins { get; set; }
            public string Name {get; set;}
            public int ItemLevel {get; set;}
            public IEnumerable<string> EquipmentTypes {get; set;}
            public int SpecialCurrencyItemID {get; set;}
            public int SpecialCurrencyCount {get; set;}
            public bool HighQualityable {get; set;}
            public bool Marketable {get; set;}
            public int GilPrice {get; set;}
            public string ClassJobRestriction {get; set;}
        }

        public struct Ingredient {
            public int IngredientID {get; set;}
            public int Quantity {get; set;}
        }

        public struct RecipeEntry {
            public int RecipeID {get; set;}
            public int CraftedItemID {get; set;}
            public int CraftedItemCount { get; set; }
            public IEnumerable<Ingredient> Ingredients {get; set;}
        }

        public static string WorldsTable() {
            return @"CREATE TABLE IF NOT EXISTS worlds (
                world_id integer PRIMARY KEY,
                name text NOT NULL,
                datacenter text NOT NULL,
                is_public boolean NOT NULL
            );";
        }
        public struct WorldEntry {
            public int WorldID {get; set;}
            public string Name {get; set;}
            public string Datacenter {get; set;}
            public bool IsPublic {get; set;}
        }

        public async static Task InsertWorldEntry(Npgsql.NpgsqlDataSource db, WorldEntry world) {
            await using var worldInsert= db.CreateCommand(buildParameterSQLInsert("worlds", 4));
            var worldIDParam = new Npgsql.NpgsqlParameter<int> {
                TypedValue = world.WorldID,
            };
            worldInsert.Parameters.Add(worldIDParam);

            var nameParam = new Npgsql.NpgsqlParameter<string> {
                TypedValue = world.Name,
            };
            worldInsert.Parameters.Add(nameParam);

            var datacenterParam = new Npgsql.NpgsqlParameter<string> {
                TypedValue = world.Datacenter,
            };
            worldInsert.Parameters.Add(datacenterParam);

            var isPublicParam = new Npgsql.NpgsqlParameter<bool> {
                TypedValue = world.IsPublic,
            };
            worldInsert.Parameters.Add(isPublicParam);

            try {
                await worldInsert.ExecuteNonQueryAsync();
                Console.WriteLine($"loaded world {world.Name}");
            }
            catch(Exception e) {
                Console.WriteLine($"Failed to load world {world.Name}: {e.Message}");
                return;
            }
        }

        public async static Task CreateWorldsTable(Npgsql.NpgsqlDataSource db) {
            await using var worldsCreate = db.CreateCommand(WorldsTable());
            await worldsCreate.ExecuteNonQueryAsync();
        }

        private static string buildParameterSQLInsert(string tableName, int parameterCount) {
            System.Text.StringBuilder sb = new System.Text.StringBuilder($"INSERT INTO {tableName} VALUES (", 100);
            for (int i = 1; i < parameterCount + 1; i += 1) {
                if (i < parameterCount) {
                    sb.Append($"${i}, ");
                } else {
                    sb.Append($"${i}");
                }
            }
            sb.Append(");");
            
            return sb.ToString();
        }
        
        async public static Task LoadItem(Npgsql.NpgsqlDataSource db, ItemEntry item) {
            await using var itemInsert= db.CreateCommand(buildParameterSQLInsert(ItemsTable(), 10));
            var itemIDParam = new Npgsql.NpgsqlParameter<int> {
                TypedValue = item.ItemID
            };
            itemInsert.Parameters.Add(itemIDParam);

            var typeParam = new Npgsql.NpgsqlParameter<string> {
                TypedValue = item.Type
            };
            itemInsert.Parameters.Add(typeParam);

            var nameParam = new Npgsql.NpgsqlParameter<string> {
                TypedValue = item.Name
            };
            itemInsert.Parameters.Add(nameParam);

            if (item.ItemLevel > 0) {
                var itemLevelParam = new Npgsql.NpgsqlParameter<int> {
                    TypedValue = item.ItemLevel,
                };
                itemInsert.Parameters.Add(itemLevelParam);
            } else {
                itemInsert.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, (object) DBNull.Value);
            }

            if (item.SpecialCurrencyItemID > 0) {
                var specialCurrencyItemIDParam = new Npgsql.NpgsqlParameter<int> {
                    TypedValue = item.SpecialCurrencyItemID,
                };
                itemInsert.Parameters.Add(specialCurrencyItemIDParam);
            } else {
                itemInsert.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, (object) DBNull.Value);
            }

            if(item.SpecialCurrencyCount > 0) {
                var specialCurrencyCountParam = new Npgsql.NpgsqlParameter<int> {
                    TypedValue = item.SpecialCurrencyCount,
                };
                itemInsert.Parameters.Add(specialCurrencyCountParam);
            } else {
                itemInsert.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, (object) DBNull.Value);
            }

            var highQualityableParam = new Npgsql.NpgsqlParameter<bool> {
                TypedValue = item.HighQualityable
            };
            itemInsert.Parameters.Add(highQualityableParam);

            var marketableParam = new Npgsql.NpgsqlParameter<bool> {
                TypedValue = item.Marketable
            };
            itemInsert.Parameters.Add(marketableParam);

            if (item.GilPrice > 0) {
                var gilPriceParam = new Npgsql.NpgsqlParameter<int> {
                    TypedValue = item.GilPrice,
                };
                itemInsert.Parameters.Add(gilPriceParam);
            } else {
                itemInsert.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, (object) DBNull.Value);
            }

            if (item.ClassJobRestriction.Length > 0) {
                var classJobParam = new Npgsql.NpgsqlParameter<string> {
                    TypedValue = item.ClassJobRestriction,
                };
                itemInsert.Parameters.Add(classJobParam);
            } else {
                itemInsert.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Text, (object) DBNull.Value);
            }

            try {
                await itemInsert.ExecuteNonQueryAsync();
            }
            catch(Exception e) { 
                Console.WriteLine($"Exception occurred while writing item {item.Name}: {e.Message}");
                return;
            }


            // Only insert child tables after the item is inserted.

            List<Task> insertions = [];

            if (item.Origins != null && item.Origins.Count() > 0 ) {
                foreach(var io in item.Origins) {
                    var insertItemOriginsCmd = db.CreateCommand(buildParameterSQLInsert(ItemOriginsTable(), 2));

                    itemIDParam = new Npgsql.NpgsqlParameter<int> {
                        TypedValue = item.ItemID
                    };
                    insertItemOriginsCmd.Parameters.Add(itemIDParam);

                    var originParam = new Npgsql.NpgsqlParameter<string> {
                        TypedValue = io
                    };
                    insertItemOriginsCmd.Parameters.Add(originParam);

                    insertions.Add(insertItemOriginsCmd.ExecuteNonQueryAsync());
                };
            }


            if (item.EquipmentTypes != null && item.EquipmentTypes.Count() > 0 ) {
                foreach(var et in item.EquipmentTypes) {
                    var insertItemEquipmentTypesCmd = db.CreateCommand(buildParameterSQLInsert(ItemEquipmentTypesTable(), 2));
                    itemIDParam = new Npgsql.NpgsqlParameter<int> {
                        TypedValue = item.ItemID
                    };
                    insertItemEquipmentTypesCmd.Parameters.Add(itemIDParam);

                    var equipmentTypeParam = new Npgsql.NpgsqlParameter<string> {
                        TypedValue = et
                    };
                    insertItemEquipmentTypesCmd.Parameters.Add(equipmentTypeParam);

                    insertions.Add(insertItemEquipmentTypesCmd.ExecuteNonQueryAsync());
                }
            }
            try {
                await Task.WhenAll(insertions);
                Console.WriteLine($"loaded item {item.Name}");
            }
            catch(Exception e) {
                Console.WriteLine($"Exception occurred while writing item subtables for item {item.Name}: {e.Message}");
            }
        }

        async public static Task LoadRecipe(Npgsql.NpgsqlDataSource db, RecipeEntry recipe) {
            var insertRecipeCmd = db.CreateCommand(buildParameterSQLInsert(RecipesTable(), 3));

            var recipeIDParam = new Npgsql.NpgsqlParameter<int> {
                TypedValue = recipe.RecipeID
            };
            insertRecipeCmd.Parameters.Add(recipeIDParam);

            var craftedItemIDParam = new NpgsqlParameter<int> {
                TypedValue = recipe.CraftedItemID
            };
            insertRecipeCmd.Parameters.Add(craftedItemIDParam);

            var craftedItemCountParam = new NpgsqlParameter<int> {
                TypedValue = recipe.CraftedItemCount
            };
            insertRecipeCmd.Parameters.Add(craftedItemCountParam);

            try {
                await insertRecipeCmd.ExecuteNonQueryAsync();
            }
            catch(Exception e) {
                Console.WriteLine($"Exception occurred while writing recipe {recipe.RecipeID}: {e.Message}");
                return;
            }

            // Subtables
            List<Task> subtableInserts = [];

            if (recipe.Ingredients == null || recipe.Ingredients.Count() == 0) {
                Console.WriteLine($"FAILED WRITE: malformed recipe {recipe.RecipeID}, no ingredients provided");
                return;
            }

            foreach(var ingredient in recipe.Ingredients) {
                var insertIngredientCmd = db.CreateCommand(buildParameterSQLInsert(RecipeIngredientsTable(), 3));

                recipeIDParam = new NpgsqlParameter<int>{
                    TypedValue = recipe.RecipeID
                };
                insertIngredientCmd.Parameters.Add(recipeIDParam);

                var ingredientIDParam = new NpgsqlParameter<int> {
                    TypedValue = ingredient.IngredientID
                };
                insertIngredientCmd.Parameters.Add(ingredientIDParam);

                var quantityParam = new NpgsqlParameter<int> {
                    TypedValue = ingredient.Quantity
                };
                insertIngredientCmd.Parameters.Add(quantityParam);

                subtableInserts.Add(insertIngredientCmd.ExecuteNonQueryAsync());
            }

            try {
                await Task.WhenAll(subtableInserts);
            }
            catch(Exception e) {
                Console.WriteLine($"Exception occurred while writing recipe subtables for recipe {recipe.RecipeID}: {e.Message}");
            }
        }

        public async static Task CreateAllTables(Npgsql.NpgsqlDataSource db) {
            var createItems = db.CreateCommand(CreateItemTable());
            await createItems.ExecuteNonQueryAsync();

            var createItemOrigins = db.CreateCommand(CreateItemOriginsTable());
            await createItemOrigins.ExecuteNonQueryAsync();

            var createItemEqTypes = db.CreateCommand(CreateItemEquipmentTypesTable());
            await createItemEqTypes.ExecuteNonQueryAsync();

            var createRecipes = db.CreateCommand(CreateRecipesTable());
            await createRecipes.ExecuteNonQueryAsync();

            var createRecipeIngredients = db.CreateCommand(CreateRecipeIngredientsTable());
            await createRecipeIngredients.ExecuteNonQueryAsync();
        }


        public async static Task DeleteAllTables(Npgsql.NpgsqlDataSource db) {
            // Sequencing is important here: items dropped last.
            var deleteItemOrigins = db.CreateCommand(DeleteItemOriginsTable());
            await deleteItemOrigins.ExecuteNonQueryAsync();

            var deleteItemEqTypes = db.CreateCommand(DeleteItemEquipmentTypesTable());
            await deleteItemEqTypes.ExecuteNonQueryAsync();

            var deleteRecipeIngredients = db.CreateCommand(DeleteRecipeIngredientsTable());
            await deleteRecipeIngredients.ExecuteNonQueryAsync();

            var deleteRecipes = db.CreateCommand(DeleteRecipesTable());
            await deleteRecipes.ExecuteNonQueryAsync();

            var deleteItems = db.CreateCommand(DeleteItemsTable());
            await deleteItems.ExecuteNonQueryAsync();
        }

        public static string CreateItemTable() {
            return @"CREATE TABLE IF NOT EXISTS items (
                item_id integer PRIMARY KEY,
                type text NOT NULL,
                name text NOT NULL,
                item_level integer,
                special_currency_item_id integer,
                special_currency_count integer,
                high_qualityable boolean NOT NULL,
                marketable boolean NOT NULL,
                gil_price integer,
                class_job_restriction text
            );";
        }

        public static string CreateItemOriginsTable() {
            return @"CREATE TABLE IF NOT EXISTS item_origins (
                item_id integer REFERENCES items ON DELETE CASCADE,
                origin text NOT NULL,
                PRIMARY KEY (item_id, origin)
            );";
        }

        public static string CreateItemEquipmentTypesTable() {
            return @"CREATE TABLE IF NOT EXISTS item_equipment_types (
                item_id integer REFERENCES items ON DELETE CASCADE,
                equipment_type text NOT NULL,
                PRIMARY KEY (item_id, equipment_type)
            );";
        }

        public static string CreateRecipesTable() {
            return @"CREATE TABLE IF NOT EXISTS recipes (
                recipe_id integer PRIMARY KEY,
                crafted_item_id integer REFERENCES items (item_id) ON DELETE CASCADE,
                crafted_item_count integer NOT NULL
            );";
        }

        public static string CreateRecipeIngredientsTable() {
            return @"CREATE TABLE IF NOT EXISTS recipe_ingredients (
                recipe_id integer REFERENCES recipes ON DELETE CASCADE, 
                ingredient_id integer REFERENCES items (item_id) ON DELETE CASCADE,
                quantity integer NOT NULL,
                PRIMARY KEY (recipe_id, ingredient_id)
            );";
        }


        public static string DeleteItemsTable() {
            return "DROP TABLE items;";
        }
        public static string DeleteItemOriginsTable() {
            return "DROP TABLE item_origins;";
        }
        public static string DeleteItemEquipmentTypesTable() {
            return "DROP TABLE item_equipment_types;";
        }
        public static string DeleteRecipesTable() {
            return "DROP TABLE recipes;";
        }
        public static string DeleteRecipeIngredientsTable() {
            return "DROP TABLE recipe_ingredients;";
        }
        public static string ItemsTable() {
            return "items";
        }
        public static string ItemOriginsTable() {
            return "item_origins";
        }
        public static string ItemEquipmentTypesTable() {
            return "item_equipment_types";
        }
        public static string RecipesTable() {
            return "recipes";
        }
        public static string RecipeIngredientsTable() {
            return "recipe_ingredients";
        }
    }
}