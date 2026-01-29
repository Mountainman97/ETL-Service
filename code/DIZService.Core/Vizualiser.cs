using SkiaSharp;
using System.Data;

namespace DIZService.Core
{
    public class Vizualiser : Helper
    {
        // spacings for borders (also between boxes vertical) and between boxes (horizontal)
        private static readonly int s_borderSpacing = 20;
        private static readonly int s_boxSpacing = 300;

        // capture the executed and expected packages of run
        private static Dictionary<string, List<string>> s_levelsFull = [];
        private static Dictionary<string, List<string>> s_levelsReal = [];

        // captures the created packages and their location
        private static Dictionary<string, Tuple<int, int>> s_packagesLoc = [];

        // pen that draws line between depending packages
        // SkiaSharp Paints for lines
        private static readonly SKPaint s_depPen = new()
        {
            Color = SKColor.Parse("#EBEBEB"),
            StrokeWidth = 2,
            IsStroke = true,
            Style = SKPaintStyle.Stroke
        };
        private static readonly SKPaint s_errorPen = new()
        {
            Color = SKColors.Red,
            StrokeWidth = 2,
            IsStroke = true,
            Style = SKPaintStyle.Stroke
        };
        private static readonly SKPaint s_errorUndonePen = new()
        {
            Color = SKColor.Parse("#8A8A8A"),
            StrokeWidth = 2,
            IsStroke = true,
            Style = SKPaintStyle.Stroke
        };
        private static readonly SKPaint s_relPen = new()
        {
            Color = SKColor.Parse("#EBEBEB"),
            StrokeWidth = 2,
            IsStroke = true,
            Style = SKPaintStyle.Stroke,
            PathEffect = SKPathEffect.CreateDash([10, 10], 0)
        };

        // SkiaSharp Paints for fills
        private static readonly SKPaint s_errorBrush = new() {
            Color = SKColor.Parse("#FF6666"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_workflowBrush = new() {
            Color = SKColor.Parse("#ff8d87"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_workflowBrushDarker = new() {
            Color = SKColor.Parse("#E0BAB9"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_undoneBrush = new() {
            Color = SKColor.Parse("#F5F5F5"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_undoneBrushDarker = new() {
            Color = SKColor.Parse("#CFCFCF"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_packageBrush = new() {
            Color = SKColor.Parse("#ffd09f"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_packageBrushDarker = new() {
            Color = SKColor.Parse("#EBD4BC"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_realizationBrush = new() {
            Color = SKColor.Parse("#caaeff"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_realizationBrushDarker = new() {
            Color = SKColor.Parse("#C5BAC9"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_stepBrush = new() {
            Color = SKColor.Parse("#b4f4b0"), Style = SKPaintStyle.Fill
        };
        private static readonly SKPaint s_stepBrushDarker = new() {
            Color = SKColor.Parse("#BECFBD"), Style = SKPaintStyle.Fill
        };

        // font within graphic
        private static readonly SKFont s_font = new () { Typeface = SKTypeface.FromFamilyName("Arial"), Size = 64 };
        private static readonly SKPaint s_fontColor = new() { Color = SKColor.Parse("#000000") };

        private readonly int _prozesslaeufeID;          // ID of process to visualize

        // will capture those modules that failed
        private static DataRow? s_errorCollection = null;

        // define size of each box
        private static readonly int s_boxwidth = 1000;
        private int _boxheight;

        // define size of image
        private int _height;
        private int _width;

        // counts the steps already printed
        private int _stepCounter;

        // use to read the prozesses etc. from DB
        private Tuple<int?, int?, int?, int?>? _prozesslaeufe;
        private readonly Workflow _workflow;
        private readonly Processor _processor;

        private int _mostWidth = 0;
        private SKBitmap? _bitmap;
        private SKCanvas? _canvas;

        public Vizualiser(Processor processor, int prozesslaeufeID, Workflow workflow)
        {
            _processor = processor;
            _workflow = workflow;
            _prozesslaeufeID = prozesslaeufeID;

            // initialize the tracking lists
            s_packagesLoc = [];
            s_levelsFull = [];
            s_levelsReal = [];
        }

        /// <summary>
        /// initialize the reading of process and the visualization
        /// </summary>
        /// <param name="prozesslaeufe">tuple that contains the prozesslaeufe IDs for vizualized workflow</param>
        public void Vizualize(Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                s_packagesLoc = [];
                s_levelsFull = [];
                s_levelsReal = [];

                _prozesslaeufe = prozesslaeufe;

                ReadRunstructureReal();
                ReadRunstructureFull();

                CreateDiagram();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Vizualize",
                    $"Failed vizualizing process!",
                    ref DummySem,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// reads the executed structure of the given process into the s_levelsReal dictionary. This includes a list
        /// of IDs for each level
        /// </summary>
        private void ReadRunstructureReal()
        {
            try
            {
                int masterPackageID = _workflow.GetMasterPackage();

                ReadDependenciesReal(masterPackageID, 1);

                s_levelsReal["Workflows"] = [$"W{_workflow.GetID()}"];

                if (_workflow.GetFallbackPackage() != -1)
                {
                    DataTable fallbackTable = _processor.DbHelper.GetDataTableFromQuery(
                       _processor,
                       $"SELECT * " +
                       $"FROM Logging.ETL_Paket_Prozesslaeufe " +
                       $"WHERE ETL_Prozesslaeufe_ID = {_prozesslaeufeID} AND " +
                       $"      ETL_Pakete_ID = {_workflow.GetFallbackPackage()}",
                       _dummyTuple
                   );

                    if (fallbackTable.Rows.Count == 1)
                    {
                        int fallbackPackageID = Convert.ToInt32(_workflow.GetFallbackPackage());
                        ReadDependenciesReal(fallbackPackageID, 1);
                    }
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadRunstructureReal",
                    $"Failed reading the process structure really executed!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// adds given package to list on given level and checks further dependencies. Finally reads realizations and
        /// their steps and writes them to their level list
        /// </summary>
        /// <param name="packageID">ID of package to check dependencies for</param>
        /// <param name="level">ID of level (depth in tree)</param>
        private void ReadDependenciesReal(int packageID, int level)
        {
            try
            {
                if (s_levelsReal.ContainsKey($"Packages{level}"))
                {
                    s_levelsReal[$"Packages{level}"].Add($"P{packageID}");
                }
                else
                {
                    s_levelsReal[$"Packages{level}"] = [$"P{packageID}"];
                }

                // read dependencies of package
                DataTable depTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * " +
                    $"FROM pc.ETL_Paket_Abhaengigkeiten AS A " +
                    $"JOIN Logging.ETL_Paket_Prozesslaeufe AS B " +
                    $"ON A.Vorlauf_ETL_Pakete_ID = B.ETL_Pakete_ID " +
                    $"WHERE A.ETL_Pakete_ID = {packageID} AND A.Ist_aktiv = 1 AND " +
                            $"B.ETL_Prozesslaeufe_ID = {_prozesslaeufeID} AND A.ETL_Workflow_ID = {_workflow.GetID()}",
                    _dummyTuple
                );

                // no dependencies found
                if (depTable.Rows.Count > 0)
                {
                    foreach (DataRow dep in depTable.Rows)
                    {
                        int vorlauf = Convert.ToInt32(dep["Vorlauf_ETL_Pakete_ID"].ToString());

                        // dependend again?
                        ReadDependenciesReal(vorlauf, level + 1);
                    }
                }

                // read realizations of package
                DataTable realizations = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * " +
                    $"FROM pc.ETL_Paket_Umsetzungen AS A " +
                    $"JOIN pc.ETL_Pakete_Paketumsetzungen AS ab " +
                    $"      ON ab.ETL_Paket_Umsetzungen_ID = A.ETL_Paket_Umsetzungen_ID " +
                    $"JOIN Logging.ETL_Paketumsetzung_Prozesslaeufe AS B " +
                    $"      ON B.ETL_Paket_Umsetzungen_ID = A.ETL_Paket_Umsetzungen_ID " +
                    $"WHERE ab.ETL_Pakete_ID = {packageID} AND " +
                          $"B.ETL_Prozesslaeufe_ID = {_prozesslaeufeID} AND " +
                          $"ab.ETL_Workflow_ID = {_workflow.GetID()} AND " +
                          $"A.Ist_Aktiv = 1 AND ab.Ist_Aktiv = 1",
                    _dummyTuple
                );

                foreach (DataRow realization in realizations.Rows)
                {
                    int umsetzungenID = Convert.ToInt32(realization["ETL_Paket_Umsetzungen_ID"].ToString());
                    if (s_levelsReal.TryGetValue($"Realizations", out var value))
                    {
                        value.Add($"R{umsetzungenID}");
                    }
                    else
                    {
                        s_levelsReal[$"Realizations"] = [$"R{umsetzungenID}"];
                    }

                    DataTable steps = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT * " +
                        $"FROM pc.ETL_Paketschritte AS A " +
                        $"JOIN pc.ETL_Paketumsetzungen_Paketschritte AS ab " +
                        $"      ON ab.ETL_Paketschritte_ID = A.ETL_Paketschritte_ID " +
                        $"JOIN Logging.ETL_Paketschritt_Prozesslaeufe AS B " +
                        $"      ON A.ETL_Paketschritte_ID = B.ETL_Paketschritte_ID " +
                        $"WHERE ab.ETL_Paket_Umsetzungen_ID = {umsetzungenID} AND " +
                                $"B.ETL_Prozesslaeufe_ID = {_prozesslaeufeID} AND " +
                                $"ab.ETL_Workflow_ID = {_workflow.GetID()} AND " +
                                $"A.Ist_Aktiv = 1 AND ab.Ist_Aktiv = 1",
                        _dummyTuple
                    );

                    List<string> stepsList = [];
                    foreach (DataRow step in steps.Rows)
                    {
                        stepsList.Add($"S{step["ETL_Paketschritte_ID"]}");
                        if (s_levelsReal.TryGetValue($"Steps", out var schritt))
                        {
                            schritt.Add($"S{step["ETL_Paketschritte_ID"]}");
                        }
                        else
                        {
                            s_levelsReal[$"Steps"] = [$"S{step["ETL_Paketschritte_ID"]}"];
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadDependenciesReal",
                    $"Failed reading dependencies for real execution!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// reads the structure of the given process that could be executed into the s_levelsReal dictionary. This
        /// includes a list of IDs for each level
        /// </summary>
        private void ReadRunstructureFull()
        {
            try
            {
                int masterPackageID = _workflow.GetMasterPackage();
                ReadDependenciesFull(masterPackageID, 1);

                if (_workflow.GetFallbackPackage() != -1)
                    ReadDependenciesFull(_workflow.GetFallbackPackage(), 1);

                s_levelsFull["Workflows"] = [$"W{_workflow.GetID()}"];
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadRunstructureFull",
                    $"Failed reading the theoretical process structure!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// adds given package to list on given level and checks further dependencies. Finally reads realizations and
        /// their steps and writes them to their level list
        /// </summary>
        /// <param name="packageID">ID of package to check dependencies for</param>
        /// <param name="level">ID of level (depth in tree)</param>
        /// <returns>dictionary that represents the dependencies</returns>
        private void ReadDependenciesFull(int packageID, int level)
        {
            try
            {
                if (s_levelsFull.ContainsKey($"Packages{level}"))
                {
                    s_levelsFull[$"Packages{level}"].Add($"P{packageID}");
                }
                else
                {
                    s_levelsFull[$"Packages{level}"] = [$"P{packageID}"];
                }

                // read dependencies of package
                DataTable depTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * " +
                    $"FROM pc.ETL_Paket_Abhaengigkeiten AS A " +
                    $"WHERE ETL_Pakete_ID = {packageID} AND Ist_aktiv = 1 AND ETL_Workflow_ID = {_workflow.GetID()}",
                    _dummyTuple
                );

                // no dependencies found
                if (depTable.Rows.Count > 0)
                {
                    foreach (DataRow dep in depTable.Rows)
                    {
                        int vorlauf = Convert.ToInt32(dep["Vorlauf_ETL_Pakete_ID"].ToString());

                        // dependend again?
                        ReadDependenciesFull(vorlauf, level + 1);
                    }
                }

                // read realizations of package
                DataTable realizations = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * " +
                    $"FROM pc.ETL_Paket_Umsetzungen AS A " +
                    $"JOIN pc.ETL_Pakete_Paketumsetzungen AS ab " +
                    $"      ON ab.ETL_Paket_Umsetzungen_ID = A.ETL_Paket_Umsetzungen_ID " +
                    $"WHERE ab.ETL_Pakete_ID = {packageID} AND ab.ETL_Workflow_ID = {_workflow.GetID()} AND " +
                    $"      A.Ist_aktiv = 1 AND ab.Ist_aktiv = 1 ",
                    _dummyTuple
                );

                foreach (DataRow realization in realizations.Rows)
                {
                    int umsetzungenID = Convert.ToInt32(realization["ETL_Paket_Umsetzungen_ID"].ToString());
                    if (s_levelsFull.TryGetValue($"Realizations", out var value))
                    {
                        value.Add($"R{umsetzungenID}");
                    }
                    else
                    {
                        s_levelsFull[$"Realizations"] = [$"R{umsetzungenID}"];
                    }

                    DataTable steps = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT * " +
                        $"FROM pc.ETL_Paketschritte AS A " +
                        $"JOIN pc.ETL_Paketumsetzungen_Paketschritte AS ab " +
                        $"      ON ab.ETL_Paketschritte_ID = A.ETL_Paketschritte_ID " +
                        $"WHERE ab.ETL_Paket_Umsetzungen_ID = {umsetzungenID} AND " +
                        $"      ab.ETL_Workflow_ID = {_workflow.GetID()} AND " +
                        $"      ab.Ist_aktiv = 1 AND A.Ist_aktiv = 1",
                        _dummyTuple
                    );

                    List<string> stepsList = [];
                    foreach (DataRow step in steps.Rows)
                    {
                        stepsList.Add($"S{step["ETL_Paketschritte_ID"]}");
                        if (s_levelsFull.TryGetValue($"Steps", out var schritt))
                        {
                            schritt.Add($"S{step["ETL_Paketschritte_ID"]}");
                        }
                        else
                        {
                            s_levelsFull[$"Steps"] = [$"S{step["ETL_Paketschritte_ID"]}"];
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadDependenciesFull",
                    $"Failed reading dependencies for full execution!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// creates the final diagram. This includes the printing of the workflow, master package and their connections
        /// as well as the definition of the width and height of the graphic
        /// </summary>
        private void CreateDiagram()
        {
            try
            {
                // what is the theoretical max. number of items on same level
                int levelCount = s_levelsReal.Keys.Count;
                foreach (var level in s_levelsFull)
                {
                    if (level.Value.Count > _mostWidth)
                        _mostWidth = level.Value.Count;
                }

                _boxheight = (int)(s_font.Size * 3 + 4 * 13);
                _height = _mostWidth * _boxheight + (_mostWidth + 1) * s_borderSpacing;
                _width = 2 * s_borderSpacing + s_boxwidth * levelCount + (levelCount - 1) * s_boxSpacing;

                _bitmap = new SKBitmap(_width, _height);
                _canvas = new SKCanvas(_bitmap);
                _canvas.Clear(SKColor.Parse("#434C5E"));

                _stepCounter = 0;

                string workflow = s_levelsReal["Workflows"][0];

                DataTable prozesslaufTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT " +
                    $"  DATEDIFF(SECOND, Ausfuehrungsstartzeitpunkt, Ausfuehrungsendzeitpunkt) " +
                    $"    AS Dauer_Exec_s, " +
                    $"  DATEDIFF(MILLISECOND, Ausfuehrungsstartzeitpunkt, Ausfuehrungsendzeitpunkt) " +
                    $"    AS Dauer_Exec_ms, " +
                    $"  Erfolgreich," +
                    $"  Parallelsperre " +
                    $"FROM Logging.ETL_Prozesslaeufe WHERE ETL_Prozesslaeufe_ID = {_prozesslaeufeID}",
                    _dummyTuple
                );

                DataTable workflowTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT Workflowname " +
                    $"FROM pc.ETL_Workflow " +
                    $"WHERE ETL_Workflow_ID = {workflow[1..]}",
                    _dummyTuple
                );

                if (prozesslaufTable.Rows.Count > 0)
                {
                    if (prozesslaufTable.Rows[0]["Erfolgreich"].ToString() == "False")
                    {
                        DataTable errorTable = _processor.DbHelper.GetDataTableFromQuery(
                            _processor,
                            $"SELECT " +
                            $"  ETL_Prozesslaeufe_ID, " +
                            $"  ETL_Paket_Prozesslaeufe_ID, " +
                            $"  ETL_Paketumsetzung_Prozesslaeufe_ID, " +
                            $"  ETL_Paketschritt_Prozesslaeufe_ID " +
                            $"FROM Logging.ETL_Fehlermeldungen " +
                            $"WHERE ETL_Prozesslaeufe_ID = {_prozesslaeufeID}",
                            _dummyTuple
                        );

                        s_errorCollection = errorTable.Rows[0];
                    }
                }

                // convert packages lists from dict to list for sorted access
                List<KeyValuePair<string, List<string>>> packagesLists = [];
                foreach (var item in s_levelsFull)
                {
                    if (item.Key[..^1] == "Packages")
                        packagesLists.Add(item);
                }

                List<string> highestPackages = packagesLists.First().Value;

                // print packages recursively
                foreach (string package in highestPackages)
                {
                    HandlePackage("", package, 2, Convert.ToInt32(workflow[1..]));
                }

                // print workflow
                List<Tuple<int, int>> highestLoc = [];

                int highestY = _height;
                int lowestY = 0;

                foreach (string package in highestPackages)
                {
                    Tuple<int, int> pack = s_packagesLoc["|" + package];
                    highestLoc.Add(pack);

                    if (pack.Item2 < highestY)
                        highestY = pack.Item2;

                    if (pack.Item2 > lowestY)
                        lowestY = pack.Item2;
                }

                int workflowY = highestY + (lowestY - highestY) / 2;
                int workflowX = 20;

                bool printedFallback = true;
                if (prozesslaufTable.Rows[0]["Erfolgreich"].ToString() == "False")
                {
                    DrawNode(
                        $"Workflow {workflow[1..]}",
                        workflowTable.Rows[0]["Workflowname"].ToString() ?? throw new ETLException("No Workflowname"),
                        "X",
                        workflowX,
                        workflowY,
                        s_workflowBrush,
                        true
                    );
                    try
                    {
                        DrawArrow(
                            workflowX + s_boxwidth,
                            workflowY + (_boxheight / 2),
                            highestLoc[1].Item1,
                            highestLoc[1].Item2,
                            s_errorPen
                        );
                    }
                    catch
                    {
                        printedFallback = false;
                        Task.Run(() => Log(
                            _processor,
                            "No Fallback package to draw",
                            _prozesslaeufe ?? throw new ETLException("No prozesslaeufe"),
                            true
                        )).Wait();
                    }
                    if (prozesslaufTable.Rows[0]["Parallelsperre"].ToString() == "True")
                        DrawPrio("L", workflowX, workflowY, s_workflowBrushDarker, false);
                }
                else
                {
                    int execTimeS = Convert.ToInt32(prozesslaufTable.Rows[0]["Dauer_Exec_s"].ToString());
                    int execTimeMS = Convert.ToInt32(prozesslaufTable.Rows[0]["Dauer_Exec_ms"].ToString());
                    string execTime = execTimeS == 0 ? $"{execTimeMS} ms" : $"{execTimeS} s";

                    DrawNode(
                        $"Workflow {workflow[1..]}",
                        workflowTable.Rows[0]["Workflowname"].ToString() ?? throw new ETLException("No Workflowname"),
                        execTime,
                        workflowX,
                        workflowY,
                        s_workflowBrush
                    );
                    try
                    {
                        DrawArrow(
                            workflowX + s_boxwidth,
                            workflowY + (_boxheight / 2),
                            highestLoc[1].Item1,
                            highestLoc[1].Item2,
                            s_errorUndonePen
                        );
                    }
                    catch
                    {
                        printedFallback = false;
                        Task.Run(() => Log(
                            _processor,
                            "No Fallback package to draw",
                            _prozesslaeufe ?? throw new ETLException("No prozesslaeufe"),
                            true
                        )).Wait();
                    }

                    if (prozesslaufTable.Rows[0]["Parallelsperre"].ToString() == "True")
                        DrawPrio("L", workflowX, workflowY, s_workflowBrushDarker, false);
                }

                if (printedFallback)
                {
                    DrawArrow(
                        workflowX + s_boxwidth,
                        workflowY + (_boxheight / 2),
                        highestLoc[0].Item1,
                        highestLoc[0].Item2 + _boxheight,
                        s_depPen
                    );
                }
                else
                {
                    DrawArrow(
                        workflowX + s_boxwidth,
                        workflowY + (_boxheight / 2),
                        highestLoc[0].Item1,
                        highestLoc[0].Item2 + (_boxheight / 2),
                        s_depPen
                    );
                }

                string file = BaseDirectory + @"..\logs\vizualizations\" + $"Visual_Process_{_prozesslaeufeID}.png";

                // check if graphics directory exists and create if not
                if (!Directory.Exists(BaseDirectory + @"..\logs\vizualizations\"))
                    Directory.CreateDirectory(BaseDirectory + @"..\logs\vizualizations\");

                // save image to logging dir
                Log(_processor, $"Saving Image to: {file}", _dummyTuple);
                using (var image = SKImage.FromBitmap(_bitmap))
                using (var data = image.Encode(SKEncodedImageFormat.Png, 400))
                using (var stream = File.OpenWrite(file))
                {
                    data.SaveTo(stream);
                }
                _bitmap.Dispose();
                _canvas.Dispose();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CreateDiagram",
                    $"Failed creating visualization of process!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// checks if the package depends on others and places them equally arround this package and finally print this
        /// package and its depending realization and steps and draws arrow between itself and the depending packages
        /// </summary>
        /// <param name="prepackage">name of parent package</param>
        /// <param name="package">name of package to check for</param>
        /// <param name="levelcount">counter for level in tree to position package</param>
        /// <param name="workflowID">ID of workflow to visualize</param>
        /// <param name="left">if true package is placed above, otherwise below (Default = true)</param>
        private void HandlePackage(string prepackage, string package, int levelcount, int workflowID, bool left = true)
        {
            try
            {
                // read depending packages
                DataTable dependenciesTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT Vorlauf_ETL_Pakete_ID " +
                    $"FROM pc.ETL_Paket_Abhaengigkeiten " +
                    $"WHERE ETL_Pakete_ID = {package[1..]} AND " +
                    $"      ETL_Workflow_ID = {workflowID} AND Ist_aktiv = 1",
                    _dummyTuple
                );

                List<string> lefts = [];
                List<string> rights = [];

                if (dependenciesTable.Rows.Count == 1)
                {
                    if (left)  // up
                    {
                        DataRow dependency = dependenciesTable.Rows[0];
                        int vorlaufpaket = Convert.ToInt32(dependency["Vorlauf_ETL_Pakete_ID"].ToString());

                        lefts.Add($"P{vorlaufpaket}");

                        // do the same for vorlaufpaket
                        HandlePackage(package, $"P{vorlaufpaket}", levelcount + 1, workflowID);

                        // do this package
                        DrawPackage(prepackage, package, levelcount, workflowID);
                    }
                    else  // right / down
                    {
                        // do this package
                        DrawPackage(prepackage, package, levelcount, workflowID);

                        DataRow dependency = dependenciesTable.Rows[0];
                        int vorlaufpaket = Convert.ToInt32(dependency["Vorlauf_ETL_Pakete_ID"].ToString());

                        rights.Add($"P{vorlaufpaket}");

                        // do the same for vorlaufpaket
                        HandlePackage(package, $"P{vorlaufpaket}", levelcount + 1, workflowID);
                    }
                }
                else
                {
                    if (dependenciesTable.Rows.Count > 0)
                    {
                        int depPackagesCount = dependenciesTable.Rows.Count / 2;

                        // left / up
                        int save = 0;
                        for (int i = 0; i < dependenciesTable.Rows.Count; i++)
                        {
                            DataRow dependency = dependenciesTable.Rows[i];
                            int vorlaufpaket = Convert.ToInt32(dependency["Vorlauf_ETL_Pakete_ID"].ToString());

                            lefts.Add($"P{vorlaufpaket}");

                            // do the same for vorlaufpaket
                            HandlePackage(package, $"P{vorlaufpaket}", levelcount + 1, workflowID, true);

                            if ((i + 1) >= depPackagesCount)
                            {
                                save = i + 1;
                                break;
                            }
                        }

                        // do this package
                        DrawPackage(prepackage, package, levelcount, workflowID);

                        // right / down
                        // handle rest of packages
                        for (int i = save; i < dependenciesTable.Rows.Count; i++)
                        {
                            DataRow dependency = dependenciesTable.Rows[i];
                            int vorlaufpaket = Convert.ToInt32(dependency["Vorlauf_ETL_Pakete_ID"].ToString());

                            rights.Add($"P{vorlaufpaket}");

                            // do the same for vorlaufpaket TODO
                            HandlePackage(package, $"P{vorlaufpaket}", levelcount + 1, workflowID, false);
                        }
                    }
                    else
                    {
                        // do this package
                        DrawPackage(prepackage, package, levelcount, workflowID);
                    }
                }

                // draw lines to depending packages
                foreach (string leftPackage in lefts)
                {
                    Tuple<int, int> start = s_packagesLoc[prepackage + "|" + package];
                    Tuple<int, int> end = s_packagesLoc[package + "|" + leftPackage];

                    int startX = start.Item1 + (int)(s_boxwidth * 0.75);
                    int startY = start.Item2;

                    int endX = end.Item1;
                    int endY = end.Item2 + _boxheight;

                    DrawArrow(startX, startY, endX, endY, s_depPen);
                }

                foreach (string rightsPackage in rights)
                {
                    Tuple<int, int> start = s_packagesLoc[prepackage + "|" + package];
                    Tuple<int, int> end = s_packagesLoc[package + "|" + rightsPackage];

                    int startX = start.Item1 + (int)(s_boxwidth * 0.75);
                    int startY = start.Item2 + _boxheight;

                    int endX = end.Item1;
                    int endY = end.Item2;

                    DrawArrow(startX, startY, endX, endY, s_depPen);
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "HandlePackage",
                    $"Failed reading package and its dependencies to print!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// draws the given packages to the canvas and lines from it to corresponding realizations
        /// </summary>
        /// <param name="prePackage">name of parent package</param>
        /// <param name="package">name of package to draw</param>
        /// <param name="levelcounter">depth in tree</param>
        /// <param name="workflowID">ID of workflow to visualize</param>
        private void DrawPackage(string prePackage, string package, int levelcounter, int workflowID)
        {
            try
            {
                // read realizations ordered by priorization
                DataTable realizationsTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT A.ETL_Paket_Umsetzungen_ID " +
                    $"FROM pc.ETL_Paket_Umsetzungen AS A " +
                    $"JOIN pc.ETL_Pakete_Paketumsetzungen AS ab " +
                    $"      ON ab.ETL_Paket_Umsetzungen_ID = A.ETL_Paket_Umsetzungen_ID " +
                    $"WHERE ab.ETL_Pakete_ID = {package[1..]} AND ab.ETL_Workflow_ID = {workflowID} AND " +
                    $"      A.Ist_aktiv = 1 AND ab.Ist_aktiv = 1 " +
                    $"ORDER BY ab.Paket_Priorisierung ASC",
                    _dummyTuple
                );

                DataTable prozesslaufTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT " +
                    $"  ETL_Paket_Prozesslaeufe_ID, " +
                    $"  DATEDIFF(SECOND, [Ausfuehrungsstartzeitpunkt], [Ausfuehrungsendzeitpunkt]) " +
                    $"    AS Dauer_Exec_s, " +
                    $"  DATEDIFF(MILLISECOND, [Ausfuehrungsstartzeitpunkt], [Ausfuehrungsendzeitpunkt]) " +
                    $"    AS Dauer_Exec_ms, " +
                    $"  Erfolgreich," +
                    $"  Parallelsperre " +
                    $"FROM Logging.ETL_Paket_Prozesslaeufe " +
                    $"WHERE ETL_Prozesslaeufe_ID = {_prozesslaeufeID} AND ETL_Pakete_ID = {package[1..]}",
                    _dummyTuple
                );

                // print realizations
                List<Tuple<int, int>> realizations = [];
                foreach (DataRow realization in realizationsTable.Rows)
                {
                    Tuple<int, int> result = DrawRealization(
                        "R" + realization["ETL_Paket_Umsetzungen_ID"].ToString(),
                        workflowID,
                        Convert.ToInt32(package[1..]),
                        prozesslaufTable.Rows.Count == 0
                    );
                    realizations.Add(new Tuple<int, int>(result.Item1, result.Item2));
                }

                DataTable packageTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT Paketname " +
                    $"FROM pc.ETL_Pakete " +
                    $"WHERE ETL_Pakete_ID = {package[1..]}",
                    _dummyTuple
                );

                // check if package had realizations and determine y coordinate of package
                int packageY;
                try
                {
                    int firstRealizationY = realizations.First().Item2;
                    int lastRealizationY = realizations.Last().Item2;
                    packageY = (lastRealizationY - firstRealizationY) / 2 + firstRealizationY;
                }
                catch
                {
                    packageY = s_borderSpacing + _stepCounter * _boxheight + _stepCounter * s_borderSpacing;
                }

                int packageX = 20 + s_boxSpacing * (levelcounter - 1) + s_boxwidth * (levelcounter - 1);

                if (prozesslaufTable.Rows.Count == 0)
                {
                    DrawNode(
                        $"Package {package[1..]}",
                        packageTable.Rows[0]["Paketname"].ToString() ?? throw new ETLException("No Paketname"),
                        "X",
                        packageX,
                        packageY,
                        s_undoneBrush
                    );
                    if (!s_packagesLoc.ContainsKey(prePackage + "|" + package))
                        s_packagesLoc.Add(prePackage + "|" + package, new Tuple<int, int>(packageX, packageY));

                    // print line from package to realizations
                    foreach (Tuple<int, int> realization in realizations)
                    {
                        int startX = packageX + s_boxwidth;
                        int startY = packageY + (_boxheight / 2);

                        int endX = realization.Item1;
                        int endY = realization.Item2 + (_boxheight / 2);

                        DrawArrow(startX, startY, endX, endY, s_relPen);
                    }

                    return;
                }
                else
                {
                    if (prozesslaufTable.Rows[0]["Erfolgreich"].ToString() == "False")
                    {
                        if ((s_errorCollection ?? throw new ETLException("No error collection"))
                            ["ETL_Paket_Prozesslaeufe_ID"].ToString() ==
                            prozesslaufTable.Rows[0]["ETL_Paket_Prozesslaeufe_ID"].ToString())
                        {
                            DrawNode(
                                $"Package {package[1..]}",
                                packageTable.Rows[0]["Paketname"].ToString() ?? throw new ETLException("No Paketname"),
                                "X",
                                packageX,
                                packageY,
                                s_errorBrush
                            );
                        }
                        else
                        {
                            DrawNode(
                                $"Package {package[1..]}",
                                packageTable.Rows[0]["Paketname"].ToString() ?? throw new ETLException("No Paketname"),
                                "X",
                                packageX,
                                packageY,
                                s_packageBrush,
                                true
                            );
                        }

                        if (!s_packagesLoc.ContainsKey(prePackage + "|" + package))
                            s_packagesLoc.Add(prePackage + "|" + package, new Tuple<int, int>(packageX, packageY));

                        // print line from package to realizations
                        foreach (Tuple<int, int> realization in realizations)
                        {
                            int startX = packageX + s_boxwidth;
                            int startY = packageY + (_boxheight / 2);

                            int endX = realization.Item1;
                            int endY = realization.Item2 + (_boxheight / 2);

                            DrawArrow(startX, startY, endX, endY, s_relPen);
                        }

                        return;
                    }
                }

                int execTimeS = Convert.ToInt32(prozesslaufTable.Rows[0]["Dauer_Exec_s"].ToString());
                int execTimeMS = Convert.ToInt32(prozesslaufTable.Rows[0]["Dauer_Exec_ms"].ToString());
                string execTime = execTimeS == 0 ? $"{execTimeMS} ms" : $"{execTimeS} s";

                // print package
                DrawNode(
                    $"Package {package[1..]}",
                    packageTable.Rows[0]["Paketname"].ToString() ?? throw new ETLException("No Paketname"),
                    execTime,
                    packageX,
                    packageY,
                    s_packageBrush
                );

                if (!s_packagesLoc.ContainsKey(prePackage + "|" + package))
                    s_packagesLoc.Add(prePackage + "|" + package, new Tuple<int, int>(packageX, packageY));

                if (prozesslaufTable.Rows[0]["Parallelsperre"].ToString() == "True")
                    DrawPrio("L", packageX, packageY, s_packageBrushDarker, false);

                // print line from package to realizations
                foreach (Tuple<int, int> realization in realizations)
                {
                    int startX = packageX + s_boxwidth;
                    int startY = packageY + (_boxheight / 2);

                    int endX = realization.Item1;
                    int endY = realization.Item2 + (_boxheight / 2);

                    DrawArrow(startX, startY, endX, endY, s_relPen);
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "DrawPackage",
                    $"Failed drawing a package and its realizations and steps!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// draws a realization with its corresponding steps to canvas
        /// </summary>
        /// <param name="realization">name of realization to draw</param>
        /// <param name="workflowID">ID of workflow to visualize</param>
        /// <param name="packageID">ID of package to visualize</param>
        /// <param name="undone">signalized if executed or not</param>
        /// <returns>Tuple that includes the X and Y coordinate of the printed relization</returns>
        private Tuple<int, int> DrawRealization(string realization, int workflowID, int packageID, bool undone)
        {
            try
            {
                // read realization steps orderd
                DataTable steps = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT A.ETL_Paketschritte_ID, A.Schrittname, ab.Schritt_Reihenfolge " +
                    $"FROM pc.ETL_Paketschritte AS A " +
                    $"JOIN pc.ETL_Paketumsetzungen_Paketschritte AS ab " +
                    $"      ON ab.ETL_Paketschritte_ID = A.ETL_Paketschritte_ID " +
                    $"WHERE ab.ETL_Paket_Umsetzungen_ID = {realization[1..]} AND " +
                    $"      ab.ETL_Workflow_ID = {workflowID} AND A.Ist_aktiv = 1  AND ab.Ist_aktiv = 1 " +
                    $"ORDER BY ab.Schritt_Reihenfolge ASC",
                    _dummyTuple
                );

                // read realizations with prio
                DataTable realizationTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT schritt.Umsetzungsname, ab.Paket_Priorisierung " +
                    $"FROM pc.ETL_Paket_Umsetzungen AS schritt " +
                    $"JOIN pc.ETL_Pakete_Paketumsetzungen AS ab " +
                    $"     ON schritt.ETL_Paket_Umsetzungen_ID = ab.ETL_Paket_Umsetzungen_ID " +
                    $"WHERE schritt.ETL_Paket_Umsetzungen_ID = {realization[1..]} AND " +
                    $"      ab.ETL_Workflow_ID = {workflowID} AND schritt.Ist_aktiv = 1 AND ab.Ist_Aktiv = 1",
                    _dummyTuple
                );

                // read realization process information (does not necessary exist)
                DataTable prozesslaufTable = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT " +
                    $"  A.ETL_Paketumsetzung_Prozesslaeufe_ID, " +
                    $"  DATEDIFF(SECOND, A.Ausfuehrungsstartzeitpunkt, A.Ausfuehrungsendzeitpunkt) " +
                    $"    AS Dauer_Exec_s, " +
                    $"  DATEDIFF(MILLISECOND, A.Ausfuehrungsstartzeitpunkt, A.Ausfuehrungsendzeitpunkt) " +
                    $"    AS Dauer_Exec_ms, " +
                    $"  A.Erfolgreich," +
                    $"  A.Parallelsperre " +
                    $"FROM Logging.ETL_Paketumsetzung_Prozesslaeufe AS A " +
                    $"JOIN Logging.ETL_Paket_Prozesslaeufe AS B " +
                    $"ON A.ETL_Prozesslaeufe_ID = B.ETL_Prozesslaeufe_ID AND " +
                       $"A.ETL_Paket_Prozesslaeufe_ID = B.ETL_Paket_Prozesslaeufe_ID " +
                    $"WHERE A.ETL_Prozesslaeufe_ID = {_prozesslaeufeID} AND " +
                          $"A.ETL_Paket_Umsetzungen_ID = {realization[1..]} AND " +
                          $"B.ETL_Pakete_ID = {packageID}",
                    _dummyTuple
                );

                int numSteps = s_levelsReal["Steps"].Count;
                bool even = numSteps % 2 == 0;

                // realization coordinations
                int realizationX;
                int realizationY;

                int realizationPrio = Convert.ToInt32(realizationTable.Rows[0]["Paket_Priorisierung"].ToString());

                // calculation coordinations of realization
                int levelcounter = s_levelsReal.Keys.ToList().Count - 1;
                if (steps.Rows.Count % 2 == 0)  // even
                {
                    int prevSteps = _stepCounter;
                    int upperBoxes = (steps.Rows.Count / 2) - 1;

                    if (upperBoxes == 0)
                    {
                        realizationY = s_borderSpacing + (_boxheight / 2) + (s_borderSpacing / 2) + prevSteps *
                                       _boxheight + prevSteps * s_borderSpacing;
                    }
                    else
                    {
                        realizationY = s_borderSpacing + (_boxheight / 2) + (s_borderSpacing / 2) + prevSteps *
                                       _boxheight + prevSteps * s_borderSpacing + upperBoxes * _boxheight + upperBoxes *
                                       s_borderSpacing;
                    }

                    realizationX = 20 + s_boxSpacing * (levelcounter - 1) + s_boxwidth * (levelcounter - 1);
                }
                else  // uneven
                {
                    int prevSteps = _stepCounter;
                    int upperBoxes = steps.Rows.Count / 2;

                    realizationY = s_borderSpacing + prevSteps * _boxheight + prevSteps * s_borderSpacing + upperBoxes *
                                   _boxheight + upperBoxes * s_borderSpacing;
                    realizationX = 20 + s_boxSpacing * (levelcounter - 1) + s_boxwidth * (levelcounter - 1);
                }

                // increase graphic size to represent the not existing steps for realization
                if (steps.Rows.Count == 0)
                {
                    realizationY = s_borderSpacing + _stepCounter * _boxheight + _stepCounter * s_borderSpacing;

                    _stepCounter++;
                    _mostWidth++;

                    _height = _mostWidth * _boxheight + (_mostWidth + 1) * s_borderSpacing;
                    // Create new SkiaSharp bitmap and canvas with increased size
                    var newBitmap = new SKBitmap(_width, _height);
                    using (var newCanvas = new SKCanvas(newBitmap))
                    {
                        newCanvas.Clear(SKColor.Parse("#434C5E"));
                        if (_bitmap != null)
                        {
                            newCanvas.DrawBitmap(_bitmap, 0, 0);
                        }
                    }
                    _bitmap = newBitmap;
                    _canvas = new SKCanvas(_bitmap);
                }

                // print steps of reralization
                levelcounter = s_levelsReal.Keys.ToList().Count;
                if (even)
                {
                    int firstY = s_borderSpacing;

                    foreach (DataRow step in steps.Rows)
                    {
                        int stepX = 20 + s_boxSpacing * (levelcounter - 1) + s_boxwidth * (levelcounter - 1);
                        int stepY = firstY + (_stepCounter * _boxheight) + (_stepCounter * s_borderSpacing);
                        string stepOrder = step["Schritt_Reihenfolge"].ToString() ??
                                                throw new ETLException("No Schritt_Reihenfolge");

                        DataTable stepProzesslaufTable = _processor.DbHelper.GetDataTableFromQuery(
                            _processor,
                            $"SELECT " +
                                $"  A.ETL_Paketschritt_Prozesslaeufe_ID, " +
                                $"  DATEDIFF(SECOND, " +
                                $"           A.Ausfuehrungsstartzeitpunkt, " +
                                $"           A.Ausfuehrungsendzeitpunkt) " +
                                $"    AS Dauer_Exec_s, " +
                                $"  DATEDIFF(MILLISECOND, " +
                                $"           A.Ausfuehrungsstartzeitpunkt, " +
                                $"           A.Ausfuehrungsendzeitpunkt) " +
                                $"    AS Dauer_Exec_ms," +
                                $"  A.Erfolgreich," +
                                $"  A.Parallelsperre " +
                                $"FROM Logging.ETL_Paketschritt_Prozesslaeufe AS A " +
                                $"JOIN Logging.ETL_Paket_Prozesslaeufe AS B " +
                                $"ON A.ETL_Prozesslaeufe_ID = B.ETL_Prozesslaeufe_ID AND " +
                                   $"A.ETL_Paket_Prozesslaeufe_ID = B.ETL_Paket_Prozesslaeufe_ID " +
                                $"WHERE A.ETL_Prozesslaeufe_ID = {_prozesslaeufeID} AND " +
                                      $"A.ETL_Paketschritte_ID = {step["ETL_Paketschritte_ID"]} AND " +
                                      $"B.ETL_Pakete_ID = {packageID}",
                            _dummyTuple
                        );

                        if (undone || stepProzesslaufTable.Rows.Count == 0)
                        {
                            DrawNode(
                                $"Step {step["ETL_Paketschritte_ID"]}",
                                step["Schrittname"].ToString() ?? throw new ETLException("No Schrittname"),
                                "X",
                                stepX,
                                stepY,
                                s_undoneBrush
                            );
                            DrawPrio(stepOrder, stepX, stepY, s_undoneBrushDarker);
                            // draw arrow from realization to step
                            int startUndoneX = realizationX + s_boxwidth;
                            int startUndoneY = realizationY + (_boxheight / 2);

                            int endUndoneX = stepX;
                            int endUndoneY = stepY + (_boxheight / 2);

                            DrawArrow(startUndoneX, startUndoneY, endUndoneX, endUndoneY, s_relPen);
                            _stepCounter++;
                            continue;
                        }
                        else
                        {
                            if (stepProzesslaufTable.Rows[0]["Erfolgreich"].ToString() == "False")
                            {
                                if ((s_errorCollection ?? throw new ETLException("No error collection"))
                                    ["ETL_Paketschritt_Prozesslaeufe_ID"].ToString() ==
                                    stepProzesslaufTable.Rows[0]["ETL_Paketschritt_Prozesslaeufe_ID"].ToString())
                                {
                                    DrawNode(
                                        $"Step {step["ETL_Paketschritte_ID"]}",
                                        step["Schrittname"].ToString() ?? throw new ETLException("No Schrittname"),
                                        "X",
                                        stepX,
                                        stepY,
                                        s_errorBrush
                                    );
                                }
                                else
                                {
                                    DrawNode(
                                        $"Step {step["ETL_Paketschritte_ID"]}",
                                        step["Schrittname"].ToString() ?? throw new ETLException("No Schrittname"),
                                        "X",
                                        stepX,
                                        stepY,
                                        s_stepBrush,
                                        true
                                    );
                                }

                                DrawPrio(stepOrder, stepX, stepY, s_undoneBrushDarker);

                                if (stepProzesslaufTable.Rows[0]["Parallelsperre"].ToString() == "True")
                                    DrawPrio("L", stepX, stepY, s_stepBrushDarker, false);

                                // draw arrow from realization to step
                                int startUndoneX = realizationX + s_boxwidth;
                                int startUndoneY = realizationY + (_boxheight / 2);

                                int endUndoneX = stepX;
                                int endUndoneY = stepY + (_boxheight / 2);

                                DrawArrow(startUndoneX, startUndoneY, endUndoneX, endUndoneY, s_relPen);
                                _stepCounter++;
                                continue;
                            }
                        }

                        int stepExecTimeS = Convert.ToInt32(stepProzesslaufTable.Rows[0]["Dauer_Exec_s"].ToString());
                        int stepExecTimeMS = Convert.ToInt32(stepProzesslaufTable.Rows[0]["Dauer_Exec_ms"].ToString());
                        string stepExecTime = stepExecTimeS == 0 ? $"{stepExecTimeMS} ms" : $"{stepExecTimeS} s";

                        DrawNode(
                            $"Step {step["ETL_Paketschritte_ID"]}",
                            step["Schrittname"].ToString() ?? throw new ETLException("No Schrittname"),
                            stepExecTime,
                            stepX,
                            stepY,
                            s_stepBrush
                        );

                        if (stepProzesslaufTable.Rows[0]["Parallelsperre"].ToString() == "True")
                            DrawPrio("L", stepX, stepY, s_stepBrushDarker, false);

                        _stepCounter++;

                        // draw arrow from realization to step
                        int startX = realizationX + s_boxwidth;
                        int startY = realizationY + (_boxheight / 2);

                        int endX = stepX;
                        int endY = stepY + (_boxheight / 2);

                        DrawArrow(startX, startY, endX, endY, s_relPen);
                        DrawPrio(stepOrder, stepX, stepY, s_stepBrushDarker);
                    }
                }
                else
                {
                    int firstY = s_borderSpacing;

                    foreach (DataRow step in steps.Rows)
                    {
                        int stepX = 20 + s_boxSpacing * (levelcounter - 1) + s_boxwidth * (levelcounter - 1);
                        int stepY = firstY + (_stepCounter * _boxheight) + (_stepCounter * s_borderSpacing);
                        string stepOrder = step["Schritt_Reihenfolge"].ToString() ??
                                                throw new ETLException("No Schritt_Reihenfolge");

                        DataTable stepProzesslaufTable = _processor.DbHelper.GetDataTableFromQuery(
                            _processor,
                            $"SELECT " +
                                $"  A.ETL_Paketschritt_Prozesslaeufe_ID, " +
                                $"  DATEDIFF(SECOND, " +
                                $"           A.Ausfuehrungsstartzeitpunkt, " +
                                $"           A.Ausfuehrungsendzeitpunkt) " +
                                $"    AS Dauer_Exec_s, " +
                                $"  DATEDIFF(MILLISECOND, " +
                                $"           A.Ausfuehrungsstartzeitpunkt, " +
                                $"           A.Ausfuehrungsendzeitpunkt) " +
                                $"    AS Dauer_Exec_ms, " +
                                $"  A.Erfolgreich," +
                                $"  A.Parallelsperre " +
                                $"FROM Logging.ETL_Paketschritt_Prozesslaeufe AS A " +
                                $"JOIN Logging.ETL_Paket_Prozesslaeufe AS B " +
                                $"ON A.ETL_Prozesslaeufe_ID = B.ETL_Prozesslaeufe_ID AND " +
                                   $"A.ETL_Paket_Prozesslaeufe_ID = B.ETL_Paket_Prozesslaeufe_ID " +
                                $"WHERE A.ETL_Prozesslaeufe_ID = {_prozesslaeufeID} AND " +
                                      $"A.ETL_Paketschritte_ID = {step["ETL_Paketschritte_ID"]} AND " +
                                      $"B.ETL_Pakete_ID = {packageID}",
                            _dummyTuple
                        );

                        if (undone || stepProzesslaufTable.Rows.Count == 0)
                        {
                            DrawNode(
                                $"Step {step["ETL_Paketschritte_ID"]}",
                                step["Schrittname"].ToString() ?? throw new ETLException("No Schrittname"),
                                "X",
                                stepX,
                                stepY,
                                s_undoneBrush
                            );
                            DrawPrio(stepOrder, stepX, stepY, s_undoneBrushDarker);
                            // draw arrow from realization to step
                            int startUndoneX = realizationX + s_boxwidth;
                            int startUndoneY = realizationY + (_boxheight / 2);

                            int endUndoneX = stepX;
                            int endUndoneY = stepY + (_boxheight / 2);

                            DrawArrow(startUndoneX, startUndoneY, endUndoneX, endUndoneY, s_relPen);
                            _stepCounter++;
                            continue;
                        }
                        else
                        {
                            if (stepProzesslaufTable.Rows[0]["Erfolgreich"].ToString() == "False")
                            {
                                if ((s_errorCollection ?? throw new ETLException("No error collection"))
                                    ["ETL_Paketschritt_Prozesslaeufe_ID"].ToString() ==
                                    stepProzesslaufTable.Rows[0]["ETL_Paketschritt_Prozesslaeufe_ID"].ToString())
                                {
                                    DrawNode(
                                        $"Step {step["ETL_Paketschritte_ID"]}",
                                        step["Schrittname"].ToString() ?? throw new ETLException("No Schrittname"),
                                        "X",
                                        stepX,
                                        stepY,
                                        s_errorBrush
                                    );
                                }
                                else
                                {
                                    DrawNode(
                                        $"Step {step["ETL_Paketschritte_ID"]}",
                                        step["Schrittname"].ToString() ?? throw new ETLException("No Schrittname"),
                                        "X",
                                        stepX,
                                        stepY,
                                        s_stepBrush,
                                        true
                                    );
                                }

                                DrawPrio(stepOrder, stepX, stepY, s_undoneBrushDarker);

                                if (stepProzesslaufTable.Rows[0]["Parallelsperre"].ToString() == "True")
                                    DrawPrio("L", stepX, stepY, s_undoneBrushDarker, false);

                                // draw arrow from realization to step
                                int startUndoneX = realizationX + s_boxwidth;
                                int startUndoneY = realizationY + (_boxheight / 2);

                                int endUndoneX = stepX;
                                int endUndoneY = stepY + (_boxheight / 2);

                                DrawArrow(startUndoneX, startUndoneY, endUndoneX, endUndoneY, s_relPen);
                                _stepCounter++;
                                continue;
                            }
                        }

                        int stepExecTimeS = Convert.ToInt32(stepProzesslaufTable.Rows[0]["Dauer_Exec_s"].ToString());
                        int stepExecTimeMS = Convert.ToInt32(stepProzesslaufTable.Rows[0]["Dauer_Exec_ms"].ToString());
                        string stepExecTime = stepExecTimeS == 0 ? $"{stepExecTimeMS} ms" : $"{stepExecTimeS} s";

                        DrawNode(
                            $"Step {step["ETL_Paketschritte_ID"]}",
                            step["Schrittname"].ToString() ?? throw new ETLException("No Schrittname"),
                            stepExecTime,
                            stepX,
                            stepY,
                            s_stepBrush
                        );
                        _stepCounter++;
                        if (stepProzesslaufTable.Rows[0]["Parallelsperre"].ToString() == "True")
                            DrawPrio("L", stepX, stepY, s_undoneBrushDarker, false);

                        // draw arrow from realization to step
                        int startX = realizationX + s_boxwidth;
                        int startY = realizationY + (_boxheight / 2);

                        int endX = stepX;
                        int endY = stepY + (_boxheight / 2);

                        DrawArrow(startX, startY, endX, endY, s_relPen);
                        DrawPrio(stepOrder, stepX, stepY, s_stepBrushDarker);
                    }
                }

                if (undone || realizationTable.Rows.Count == 0)
                {
                    DrawNode(
                        $"Realization {realization[1..]}",
                        realizationTable.Rows[0]["Umsetzungsname"].ToString() ??
                                throw new ETLException("No Umsetzungsname"),
                        "X",
                        realizationX,
                        realizationY,
                        s_undoneBrush);
                    DrawPrio($"{realizationPrio}", realizationX, realizationY, s_undoneBrushDarker);

                    return new Tuple<int, int>(realizationX, realizationY);
                }
                else
                {
                    if (prozesslaufTable.Rows.Count > 0)
                    {
                        if (prozesslaufTable.Rows[0]["Erfolgreich"].ToString() == "False")
                        {
                            if ((s_errorCollection ?? throw new ETLException("No error collection"))
                                ["ETL_Paketumsetzung_Prozesslaeufe_ID"].ToString() ==
                                prozesslaufTable.Rows[0]["ETL_Paketumsetzung_Prozesslaeufe_ID"].ToString())
                            {
                                DrawNode(
                                    $"Realization {realization[1..]}",
                                    realizationTable.Rows[0]["Umsetzungsname"].ToString() ??
                                            throw new ETLException("No Umsetzungsname"),
                                    "X",
                                    realizationX,
                                    realizationY,
                                    s_errorBrush
                                );
                            }
                            else
                            {
                                DrawNode(
                                    $"Realization {realization[1..]}",
                                    realizationTable.Rows[0]["Umsetzungsname"].ToString() ??
                                            throw new ETLException("No Umsetzungsname"),
                                    "X",
                                    realizationX,
                                    realizationY,
                                    s_realizationBrush,
                                    true
                                );
                            }

                            DrawPrio($"{realizationPrio}", realizationX, realizationY, s_undoneBrushDarker);
                            if (prozesslaufTable.Rows[0]["Parallelsperre"].ToString() == "True")
                                DrawPrio("L", realizationX, realizationY, s_undoneBrushDarker, false);

                            return new Tuple<int, int>(realizationX, realizationY);
                        }
                    }
                }

                int execTimeS = 0;
                int execTimeMS = 0;
                string parallellock = "False";
                SKPaint brush = s_undoneBrush;
                SKPaint brushPrio = s_undoneBrushDarker;

                if (prozesslaufTable.Rows.Count > 0)
                {
                    execTimeS = Convert.ToInt32(prozesslaufTable.Rows[0]["Dauer_Exec_s"].ToString());
                    execTimeMS = Convert.ToInt32(prozesslaufTable.Rows[0]["Dauer_Exec_ms"].ToString());
                    parallellock = prozesslaufTable.Rows[0]["Parallelsperre"].ToString() ??
                                        throw new ETLException("No Parallelsperre");
                    brush = s_realizationBrush;
                    brushPrio = s_realizationBrushDarker;
                }

                string execTime = execTimeS == 0 ? $"{execTimeMS} ms" : $"{execTimeS} s";

                DrawNode(
                    $"Realization {realization[1..]}",
                    realizationTable.Rows[0]["Umsetzungsname"].ToString() ??
                            throw new ETLException("No Umsetzungsname"),
                    execTime,
                    realizationX,
                    realizationY,
                    brush
                );
                DrawPrio($"{realizationPrio}", realizationX, realizationY, brushPrio);
                if (parallellock == "True")
                    DrawPrio("L", realizationX, realizationY, brushPrio, false);

                return new Tuple<int, int>(realizationX, realizationY);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "DrawRealization",
                    $"Failed drawing a realization and its steps!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// draws a box with given labels (3 lines) to canvas to given x and y coordinates
        /// </summary>
        /// <param name="label_up">name of module</param>
        /// <param name="label_mid">description of module</param>
        /// <param name="lable_down">execution time of module</param>
        /// <param name="x">x coordinate of box</param>
        /// <param name="y">y coordinate of box</param>
        /// <param name="brush">the brush to draw the box with</param>
        /// <param name="error">true if this module failed</param>
        private void DrawNode(
            string label_up,
            string label_mid,
            string lable_down,
            int x,
            int y,
            SKPaint brush,
            bool error = false
        )
        {
            try
            {
                int width = s_boxwidth;  // 300
                int height = _boxheight;
                var rect = new SKRect(x, y, x + width, y + height);

                // Rechteck füllen
                (_canvas ?? throw new ETLException("Canvas was not initialized!")).DrawRect(rect, brush);

                // Rahmen zeichnen
                if (error)
                {
                    using var borderPaint = new SKPaint
                    {
                        Color = SKColors.Red,
                        Style = SKPaintStyle.Stroke,
                        StrokeWidth = 3
                    };
                    _canvas.DrawRect(rect, borderPaint);
                }
                else
                {
                    using var borderPaint = new SKPaint
                    {
                        Color = SKColors.Black,
                        Style = SKPaintStyle.Stroke,
                        StrokeWidth = 1
                    };
                    _canvas.DrawRect(rect, borderPaint);
                }

                // Text zentrieren und zeichnen
                float textY = y + 5 + s_font.Size;
                float textX;

                // 1. Zeile (oben, fett)
                var headFont = new SKFont(s_font.Typeface, s_font.Size)
                {
                    Edging = s_font.Edging,
                    Hinting = s_font.Hinting,
                    Subpixel = s_font.Subpixel,
                    LinearMetrics = s_font.LinearMetrics,
                    Embolden = true
                };

                // SKPaint zur Messung erzeugen
                var measurePaint = new SKFont
                {
                    Typeface = headFont.Typeface,
                    Size = headFont.Size
                };
                // Textbreite berechnen
                float textWidth = measurePaint.MeasureText(label_up);
                // Zentrierung berechnen
                textX = x + (width - textWidth) / 2;
                // TextBlob erzeugen und zeichnen
                using var blob = SKTextBlob.Create(label_up, headFont);
                _canvas.DrawText(blob, textX, textY, s_fontColor);

                // 2. Zeile (Mitte)
                measurePaint = new SKFont
                {
                    Typeface = s_font.Typeface,
                    Size = s_font.Size
                };
                // Textbreite berechnen
                textWidth = measurePaint.MeasureText(label_mid);
                // Zentrierung berechnen
                textX = x + (width - textWidth) / 2;
                // TextBlob erzeugen und zeichnen
                using var blob2 = SKTextBlob.Create(label_mid, s_font);
                _canvas.DrawText(blob2, textX, textY + 15 + s_font.Size, s_fontColor);

                // 3. Zeile (unten)
                // Textbreite berechnen
                textWidth = measurePaint.MeasureText(lable_down);
                // Zentrierung berechnen
                textX = x + (width - textWidth) / 2;
                // TextBlob erzeugen und zeichnen
                using var blob3 = SKTextBlob.Create(lable_down, s_font);
                _canvas.DrawText(blob3, textX, textY + 15 + s_font.Size + 15 + s_font.Size, s_fontColor);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "DrawNode",
                    $"Failed drawing a node to canvas!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// draws an arrow from one point to another in a given style
        /// </summary>
        /// <param name="x1">x coordinate of origin</param>
        /// <param name="y1">y coordinate of origin</param>
        /// <param name="x2">x coordinate of target</param>
        /// <param name="y2">y coordinate of target</param>
        /// <param name="pen">style to draw line/arrow</param>
        private void DrawArrow(int x1, int y1, int x2, int y2, SKPaint pen)
        {
            try
            {
                //draw line with arrow
                (_canvas ?? throw new ETLException("Canvas was not initialized")).DrawLine(
                    new SKPoint(x1, y1),
                    new SKPoint(x2, y2),
                    pen
                );
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "DrawArrow",
                    $"Failed drawing an arrow to canvas!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }

        /// <summary>
        /// draws a circle with a given Label (mostly a number) to a given corner of a box. based on parameter right
        /// this circle will be placed in upper left or upper right corner
        /// </summary>
        /// <param name="label">label to print inside circle</param>
        /// <param name="x">x coordinate of node</param>
        /// <param name="y">y coordinate of node</param>
        /// <param name="brush">style to print circle in</param>
        /// <param name="right">positions to upper right when true, otherwise upper left (Default = true)</param>
        private void DrawPrio(string label, int x, int y, SKPaint brush, bool right = true)
        {
            try
            {
                int width = (int)(s_font.Size * 1 + 2 * 5);
                int height = (int)(s_font.Size * 1 + 2 * 5);

                float rectX, rectY;
                if (right)
                {
                    rectX = x + s_boxwidth - (float)(width * 0.75);
                    rectY = y - (float)(height * 0.25);
                }
                else
                {
                    rectX = x - (float)(width * 0.25);
                    rectY = y - (float)(height * 0.25);
                }

                var rect = new SKRect(rectX, rectY, rectX + width, rectY + height);

                // draw filled ellipse (circle)
                (_canvas ?? throw new ETLException("Canvas was not initialized!")).DrawOval(rect, brush);

                // draw ellipse border
                using (var borderPaint = new SKPaint {
                    Color = SKColors.Black, Style = SKPaintStyle.Stroke, StrokeWidth = 2
                })
                {
                    _canvas.DrawOval(rect, borderPaint);
                }

                // centralize text in node
                // Text-Messung vorbereiten
                var headFont = new SKFont(s_font.Typeface, s_font.Size - 10)
                {
                    Embolden = true
                };

                // Textgröße berechnen
                var textBounds = new SKRect();
                headFont.MeasureText(label, out textBounds);

                // Position berechnen für zentrierten Text
                float stringX = rect.MidX - textBounds.Width / 2 - textBounds.Left;
                float stringY = rect.MidY - textBounds.Height / 2 - textBounds.Top;

                // TextBlob erzeugen und zeichnen
                using var blob = SKTextBlob.Create(label, headFont);
                _canvas.DrawText(blob, stringX, stringY, s_fontColor);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "DrawPrio",
                    $"Failed drawing a circle with label to canvas!",
                    ref DummySem,
                    _prozesslaeufe ?? throw new ETLException("No prozesslaeufe")
                );
            }
        }
    }
}
